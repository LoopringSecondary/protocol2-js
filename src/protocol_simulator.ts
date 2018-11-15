import { BigNumber } from "bignumber.js";
import BN = require("bn.js");
import { BalanceBook } from "./balance_book";
import { Bitstream } from "./bitstream";
import { Context } from "./context";
import { ensure } from "./ensure";
import { ExchangeDeserializer } from "./exchange_deserializer";
import { Mining } from "./mining";
import { OrderUtil } from "./order";
import { Ring } from "./ring";
import { InvalidRingEvent, OrderInfo, RingMinedEvent, RingsInfo, SimulatorReport, Spendable,
         TokenType, TransactionPayments, TransferItem } from "./types";
import { xor } from "./xor";

export class ProtocolSimulator {

  public context: Context;
  public offLineMode: boolean = false;

  private orderUtil: OrderUtil;

  constructor(context: Context) {
    this.context = context;
    this.orderUtil = new OrderUtil(context);
  }

  public deserialize(data: string,
                     transactionOrigin: string) {
    const exchangeDeserializer = new ExchangeDeserializer(this.context);
    const [mining, orders, rings] = exchangeDeserializer.deserialize(data);

    const ringsInfo: RingsInfo = {
      rings,
      orders,
      feeRecipient: mining.feeRecipient,
      miner: mining.miner,
      sig: mining.sig,
      transactionOrigin,
    };
    return ringsInfo;
  }

  public async simulateAndReport(ringsInfo: RingsInfo) {

    const mining = new Mining(
      this.context,
      ringsInfo.feeRecipient ? ringsInfo.feeRecipient : ringsInfo.transactionOrigin,
      ringsInfo.miner,
      ringsInfo.sig,
    );

    const orders = ringsInfo.orders;

    const rings: Ring[] = [];
    for (const indexes of ringsInfo.rings) {
      const ringOrders: OrderInfo[] = [];
      for (const orderIndex of indexes) {
        const orderInfo = ringsInfo.orders[orderIndex];
        ringOrders.push(orderInfo);
      }
      const ring = new Ring(
        this.context,
        ringOrders,
      );
      rings.push(ring);
    }

    for (const order of orders) {
      order.valid = true;
      await this.orderUtil.validateInfo(order);
      this.orderUtil.checkP2P(order);
      order.hash = this.orderUtil.getOrderHash(order);
      await this.orderUtil.updateBrokerAndInterceptor(order);
    }
    await this.batchGetFilledAndCheckCancelled(orders);
    this.updateBrokerSpendables(orders);
    for (const order of orders) {
      await this.orderUtil.checkBrokerSignature(order);
    }

    for (const ring of rings) {
      ring.updateHash();
    }

    mining.updateHash(rings);
    await mining.updateMinerAndInterceptor();
    assert(mining.checkMinerSignature(ringsInfo.transactionOrigin) === true,
           "INVALID_SIG");

    for (const order of orders) {
      this.orderUtil.checkDualAuthSignature(order, mining.hash);
    }

    const ringMinedEvents: RingMinedEvent[] = [];
    const invalidRingEvents: InvalidRingEvent[] = [];
    const transferItems: TransferItem[] = [];
    const feeBalances = new BalanceBook();
    for (const ring of rings) {
      ring.checkOrdersValid();
      ring.checkForSubRings();
      await ring.calculateFillAmountAndFee();
      if (ring.valid) {
        ring.adjustOrderStates();
      }
    }

    // Check if the allOrNone orders are completely filled over all rings
    // This can invalidate rings
    this.checkRings(orders, rings);

    for (const ring of rings) {
      if (ring.valid) {
        const ringReport = await this.simulateAndReportSingle(ring, mining, feeBalances);
        ringMinedEvents.push(ringReport.ringMinedEvent);
        // Merge transfer items if possible
        for (const ringTransferItem of ringReport.transferItems) {
          let addNew = true;
          for (const transferItem of transferItems) {
            if (transferItem.token === ringTransferItem.token &&
                transferItem.from === ringTransferItem.from &&
                transferItem.to === ringTransferItem.to &&
                transferItem.tokenType === ringTransferItem.tokenType &&
                transferItem.fromTranche === ringTransferItem.fromTranche &&
                transferItem.data === ringTransferItem.data) {
                transferItem.amount = transferItem.amount.plus(ringTransferItem.amount);
                addNew = false;
            }
          }
          if (addNew) {
            transferItems.push(ringTransferItem);
          }
        }
      } else {
        const invalidRingEvent: InvalidRingEvent = {
          ringHash: "0x" + ring.hash.toString("hex"),
        };
        invalidRingEvents.push(invalidRingEvent);
      }
    }

    const report = await this.collectReport(ringsInfo,
                                           mining,
                                           rings,
                                           transferItems,
                                           feeBalances,
                                           ringMinedEvents,
                                           invalidRingEvents);

    await this.validateRings(ringsInfo, report);

    return report;
  }

  private async checkRings(orders: OrderInfo[], rings: Ring[]) {
    // Check if allOrNone orders are completely filled
    // When a ring is turned invalid because of an allOrNone order we have to
    // recheck the other rings again because they may contain other allOrNone orders
    // that may not be completely filled anymore.
    let reevaluateRings = true;
    while (reevaluateRings) {
      reevaluateRings = false;
      for (const order of orders) {
        // Check if this order needs to be completely filled
        if (order.allOrNone) {
          const validBefore = order.valid;
          this.orderUtil.validateAllOrNone(order);
          // Check if the order valid status has changed
          reevaluateRings = reevaluateRings || (validBefore !== order.valid);
        }
      }
      if (reevaluateRings) {
        for (const ring of rings) {
          const validBefore = ring.valid;
          ring.checkOrdersValid();
          // If the ring was valid before the completely filled check we have to revert the filled amountS
          // of the orders in the ring. This is a bit awkward so maybe there's a better solution.
          if (!ring.valid && validBefore) {
            ring.revertOrderStats();
          }
        }
      }
    }
  }

  private async simulateAndReportSingle(ring: Ring, mining: Mining, feeBalances: BalanceBook) {
    const transferItems = await ring.doPayments(mining, feeBalances);
    const fills = ring.generateFills();
    const ringMinedEvent: RingMinedEvent = {
      ringIndex: new BigNumber(this.context.ringIndex++),
      ringHash: "0x" + ring.hash.toString("hex"),
      feeRecipient: mining.feeRecipient,
      fills,
    };
    return {ringMinedEvent, transferItems};
  }

  private async batchGetFilledAndCheckCancelled(orders: OrderInfo[]) {
    const bitstream = new Bitstream();
    for (const order of orders) {
      bitstream.addAddress(order.broker, 32);
      bitstream.addAddress(order.owner, 32);
      bitstream.addHex(order.hash.toString("hex"));
      bitstream.addNumber(order.validSince, 32);
      bitstream.addHex(xor(order.tokenS, order.tokenB, 20));
      bitstream.addNumber(0, 12);
    }

    const fills = await this.context.tradeDelegate.batchGetFilledAndCheckCancelled(bitstream.getBytes32Array());

    const cancelledValue = new BigNumber("F".repeat(64), 16);
    for (const [i, order] of orders.entries()) {
      order.filledAmountS = fills[i];
      order.valid = order.valid && ensure(!fills[i].equals(cancelledValue), "order is cancelled");
    }
  }

  private updateBrokerSpendables(orders: OrderInfo[]) {
    // Spendables for brokers need to be setup just right for the allowances to work, we cannot trust
    // the miner to do this for us. Spendables for tokens don't need to be correct, if they are incorrect
    // the transaction will fail, so the miner will want to send those correctly.
    interface BrokerSpendable {
      broker: string;
      owner: string;
      token: string;
      spendable: Spendable;
    }

    const brokerSpendables: BrokerSpendable[] = [];
    const addBrokerSpendable = (broker: string, owner: string, token: string) => {
      // Find an existing one
      for (const spendable of brokerSpendables) {
        if (spendable.broker === broker && spendable.owner === owner && spendable.token === token) {
          return spendable.spendable;
        }
      }
      // Create a new one
      const newSpendable = {
        initialized: false,
        amount: new BigNumber(0),
        reserved: new BigNumber(0),
      };
      const newBrokerSpendable = {
        broker,
        owner,
        token,
        spendable: newSpendable,
      };
      brokerSpendables.push(newBrokerSpendable);
      return newBrokerSpendable.spendable;
    };

    for (const order of orders) {
      if (order.brokerInterceptor) {
        order.brokerSpendableS = addBrokerSpendable(order.broker, order.owner, order.tokenS);
        order.brokerSpendableFee = addBrokerSpendable(order.broker, order.owner, order.feeToken);
      }
    }
  }

  private async collectReport(ringsInfo: RingsInfo,
                              mining: Mining,
                              rings: Ring[],
                              transferItems: TransferItem[],
                              feeBalances: BalanceBook,
                              ringMinedEvents: RingMinedEvent[],
                              invalidRingEvents: InvalidRingEvent[]) {
    const orders = ringsInfo.orders;
    const zeroAddress = "0x" + "0".repeat(64);

    // Collect balances before the transaction
    const balancesBefore = new BalanceBook();
    for (const order of orders) {
      if (!balancesBefore.isBalanceKnown(order.owner, order.tokenS, order.trancheS)) {
        const amount = await this.orderUtil.getTokenSpendable(order.tokenTypeS,
                                                              order.tokenS,
                                                              order.trancheS,
                                                              order.owner);
        balancesBefore.addBalance(order.owner, order.tokenS, order.trancheS, amount);
      }
      if (!balancesBefore.isBalanceKnown(order.tokenRecipient, order.tokenB, order.trancheB)) {
        const amount = await this.orderUtil.getTokenSpendable(order.tokenTypeB,
                                                              order.tokenB,
                                                              order.trancheB,
                                                              order.tokenRecipient);
        balancesBefore.addBalance(order.tokenRecipient, order.tokenB, order.trancheB, amount);
      }
      if (!balancesBefore.isBalanceKnown(order.owner, order.feeToken, zeroAddress)) {
        const amount = await this.orderUtil.getTokenSpendable(order.tokenTypeFee,
                                                              order.feeToken,
                                                              zeroAddress,
                                                              order.owner);
        balancesBefore.addBalance(order.owner, order.feeToken, zeroAddress, amount);
      }
    }
    for (const order of orders) {
      if (order.tokenTypeS === TokenType.ERC20) {
        const Token = this.context.ERC20Contract.at(order.tokenS);
        // feeRecipient
        if (!balancesBefore.isBalanceKnown(mining.feeRecipient, order.tokenS, order.trancheS)) {
          const amount = await Token.balanceOf(mining.feeRecipient);
          balancesBefore.addBalance(mining.feeRecipient, order.tokenS, order.trancheS, amount);
        }
      }
    }

    // Simulate the token transfers of all rings
    const balancesAfter = balancesBefore.copy();
    for (const transfer of transferItems) {
      balancesAfter.addBalance(transfer.from, transfer.token, transfer.fromTranche, transfer.amount.neg());
      balancesAfter.addBalance(transfer.to, transfer.token, transfer.toTranche, transfer.amount);
    }

    // Get the fee balances before the transaction
    const feeBalancesBefore = new BalanceBook();
    const feeHolder = this.context.feeHolder.address;
    for (const order of orders) {
      const tokens = [order.tokenS, order.tokenB, order.feeToken];
      const feeRecipients = [order.owner, order.walletAddr, mining.feeRecipient, feeHolder];
      for (const token of tokens) {
        for (const feeRecipient of feeRecipients) {
          if (feeRecipient && !feeBalancesBefore.isBalanceKnown(feeRecipient, token, zeroAddress)) {
            feeBalancesBefore.addBalance(feeRecipient,
                                         token,
                                         zeroAddress,
                                         await this.context.feeHolder.feeBalances(token, feeRecipient));
          }
        }
      }
    }

    // Calculate the balances after the transaction
    const feeBalancesAfter = feeBalancesBefore.copy();
    for (const balance of feeBalances.getAllBalances()) {
      feeBalancesAfter.addBalance(balance.owner, balance.token, balance.tranche, balance.amount);
    }

    // Get the filled amounts before
    const filledAmountsBefore: { [hash: string]: BigNumber; } = {};
    for (const order of orders) {
      const orderHash = order.hash.toString("hex");
      filledAmountsBefore[orderHash] = await this.context.tradeDelegate.filled("0x" + orderHash);
    }

    // Filled amounts after
    const filledAmountsAfter: { [hash: string]: BigNumber; } = {};
    for (const order of orders) {
      const orderHash = order.hash.toString("hex");
      let filledAmountS = order.filledAmountS ? order.filledAmountS : new BigNumber(0);
      if (!order.valid) {
        filledAmountS = filledAmountsBefore[orderHash];
      }
      filledAmountsAfter[orderHash] = filledAmountS;
    }

    // Collect the payments
    const payments: TransactionPayments = {
      rings: [],
    };
    for (const ring of rings) {
      payments.rings.push(ring.payments);
    }

    // Create the report
    const simulatorReport: SimulatorReport = {
      reverted: false,
      ringMinedEvents,
      invalidRingEvents,
      transferItems,
      feeBalancesBefore,
      feeBalancesAfter,
      filledAmountsBefore,
      filledAmountsAfter,
      balancesBefore,
      balancesAfter,
      payments,
    };
    return simulatorReport;
  }

  private async validateRings(ringsInfo: RingsInfo,
                              report: SimulatorReport) {
    const orders = ringsInfo.orders;
    const zeroAddress = "0x" + "0".repeat(64);

    // Check if we haven't spent more funds than the owner owns
    for (const balance of report.balancesAfter.getAllBalances()) {
      assert(balance.amount.gte(0), "can't sell more tokens than the owner owns");
    }

    // Check if the spendables were updated correctly
    for (const order of orders) {
      if (order.tokenSpendableS.initialized) {
        let amountTransferredS = new BigNumber(0);
        for (const transfer of report.transferItems) {
          if (transfer.from === order.owner &&
              transfer.token === order.tokenS &&
              transfer.fromTranche === order.trancheS) {
            amountTransferredS = amountTransferredS.plus(transfer.amount);
          }
        }
        const amountSpentS = order.tokenSpendableS.initialAmount.minus(order.tokenSpendableS.amount);
        // amountTransferred could be less than amountSpent because of rebates
        assert(amountSpentS.gte(amountTransferredS), "amountSpentS >= amountTransferredS");
      }
      if (order.tokenSpendableFee.initialized) {
        let amountTransferredFee = new BigNumber(0);
        for (const transfer of report.transferItems) {
          if (transfer.from === order.owner &&
              transfer.token === order.feeToken &&
              transfer.fromTranche === zeroAddress) {
            amountTransferredFee = amountTransferredFee.plus(transfer.amount);
          }
        }
        const amountSpentFee = order.tokenSpendableFee.initialAmount.minus(order.tokenSpendableFee.amount);
        // amountTransferred could be less than amountSpent because of rebates
        assert(amountSpentFee.gte(amountTransferredFee), "amountSpentFee >= amountTransferredFee");
      }
    }

    // Check if the allOrNone orders were correctly filled
    for (const order of orders) {
      if (order.allOrNone) {
        assert(order.filledAmountS.eq(0) || order.filledAmountS.eq(order.amountS),
               "allOrNone orders should either be completely filled or not at all.");
      }
    }
  }
}
