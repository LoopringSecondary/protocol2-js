import { BigNumber } from "bignumber.js";
import ABI = require("ethereumjs-abi");
import { BalanceBook } from "./balance_book";
import { Bitstream } from "./bitstream";
import { Context } from "./context";
import { ensure } from "./ensure";
import { logDebug } from "./logs";
import { Mining } from "./mining";
import { OrderUtil } from "./order";
import { DetailedTokenTransfer, Fill, OrderInfo, OrderPayments, Participation,
         RingPayments, TokenType, TransferItem } from "./types";

export class Ring {

  public participations: Participation[] = [];
  public hash?: Buffer;
  public minerFeesToOrdersPercentage?: number;
  public valid: boolean;

  public payments: RingPayments;

  private context: Context;
  private orderUtil: OrderUtil;

  private feeBalances = new BalanceBook();

  private zeroAddress = "0x" + "0".repeat(64);

  // BEGIN diagnostics
  private detailTransferS: DetailedTokenTransfer[];
  private detailTransferB: DetailedTokenTransfer[];
  private detailTransferFee: DetailedTokenTransfer[];
  // END diagnostics

  constructor(context: Context,
              orders: OrderInfo[]) {
    this.context = context;
    this.valid = true;
    this.minerFeesToOrdersPercentage = 0;
    this.orderUtil = new OrderUtil(context);

    for (const order of orders) {
      const participation: Participation = {
        order,
        splitS: new BigNumber(0),
        feeAmount: new BigNumber(0),
        feeAmountS: new BigNumber(0),
        feeAmountB: new BigNumber(0),
        rebateFee: new BigNumber(0),
        rebateS: new BigNumber(0),
        rebateB: new BigNumber(0),
        fillAmountS: new BigNumber(0),
        fillAmountB: new BigNumber(0),
        ringSpendableS: new BigNumber(0),
        ringSpendableFee: new BigNumber(0),
      };
      this.participations.push(participation);
    }

    // BEGIN diagnostics
    this.payments = {
      orders: [],
    };
    this.detailTransferS = [];
    this.detailTransferB = [];
    this.detailTransferFee = [];
    for (const [i, order] of orders.entries()) {
      this.payments.orders.push({
        payments: [],
      });

      const prevOrder = orders[(i + orders.length - 1) % orders.length];
      const nextOrder = orders[(i + 1) % orders.length];

      const paymentS: DetailedTokenTransfer = {
        description: "Sell",
        token: order.tokenS,
        from: order.owner,
        to: prevOrder.owner,
        amount: 0,
        subPayments: [],
      };
      this.detailTransferS.push(paymentS);
      this.payments.orders[i].payments.push(paymentS);

      const paymentB: DetailedTokenTransfer = {
        description: "Buy",
        token: order.tokenB,
        from: nextOrder.owner,
        to: order.owner,
        amount: 0,
        subPayments: [],
      };
      this.detailTransferB.push(paymentB);
      this.payments.orders[i].payments.push(paymentB);

      const paymentFee: DetailedTokenTransfer = {
        description: "MatchingFee",
        token: order.feeToken,
        from: order.owner,
        to: "NA",
        amount: 0,
        subPayments: [],
      };
      this.detailTransferFee.push(paymentFee);
      this.payments.orders[i].payments.push(paymentFee);
    }
    // END diagnostics
  }

  public updateHash() {
    const orderHashes = new Bitstream();
    for (const p of this.participations) {
      orderHashes.addHex(p.order.hash.toString("hex"));
      orderHashes.addNumber(p.order.waiveFeePercentage ? p.order.waiveFeePercentage : 0, 2);
    }
    this.hash = ABI.soliditySHA3(["bytes"], [Buffer.from(orderHashes.getData().slice(2), "hex")]);
  }

  public checkOrdersValid() {
    this.valid = this.valid &&
                 ensure(this.participations.length > 1 && this.participations.length <= 8, "invald ring size");
    for (let i = 0; i < this.participations.length; i++) {
      const prevIndex = (i + this.participations.length - 1) % this.participations.length;
      this.valid = this.valid && ensure(this.participations[i].order.valid, "ring contains invalid order");
      this.valid = this.valid &&
                   ensure(this.participations[i].order.tokenS === this.participations[prevIndex].order.tokenB,
                          "tokenS/tokenB mismatch");
      this.valid = this.valid &&
                   ensure(this.participations[i].order.tokenTypeS === this.participations[prevIndex].order.tokenTypeB,
                          "tokenTypeS/tokenTypeB mismatch");
    }
  }

  public checkForSubRings() {
    for (let i = 0; i < this.participations.length - 1; i++) {
      const tokenS = this.participations[i].order.tokenS;
      for (let j = i + 1; j < this.participations.length; j++) {
        this.valid = this.valid && ensure(tokenS !== this.participations[j].order.tokenS, "ring has sub-rings");
      }
    }
  }

  public async calculateFillAmountAndFee() {
    // Invalid order data could cause a divide by zero in the calculations
    if (!this.valid) {
      return;
    }

    for (const p of this.participations) {
      await this.setMaxFillAmounts(p);
    }

    let smallest = 0;
    const ringSize = this.participations.length;

    for (let i = ringSize - 1; i >= 0; i--) {
      smallest = this.resize(i, smallest);
    }

    for (let i = ringSize - 1; i >= smallest; i--) {
      this.resize(i, smallest);
    }

    // Reserve the total amount tokenS used for all the orders
    // (e.g. the owner of order 0 could use LRC as feeToken in order 0, while
    // the same owner can also sell LRC in order 2).
    for (const p of this.participations) {
      await this.orderUtil.reserveAmountS(p.order, p.fillAmountS);
    }

    for (let i = 0; i < ringSize; i++) {
      const prevIndex = (i + ringSize - 1) % ringSize;

      // Check if we can transfer the tokens
      if (this.participations[i].order.tokenTypeS === TokenType.ERC1400) {
        const ERC1400token = this.context.ERC1400Contract.at(this.participations[i].order.tokenS);
        const [ESC, unused, destTranche] = await ERC1400token.canSend(
            this.participations[i].order.owner,
            this.participations[prevIndex].order.tokenRecipient,
            this.participations[i].order.trancheS,
            this.participations[i].fillAmountS,
            this.participations[i].order.transferDataS ? this.participations[i].order.transferDataS : "0x0",
        );
        this.valid = this.valid && ensure(ESC === "0xa0" || ESC === "0xa1" || ESC === "0xa2", "canSend is false");
        this.valid = this.valid &&
                     ensure(destTranche === this.participations[prevIndex].order.trancheB, "wrong dest tranche");
      }

      const valid = await this.calculateFees(this.participations[i], this.participations[prevIndex]);
      this.valid = this.valid && ensure(valid, "ring cannot be settled");
      if (this.participations[i].order.waiveFeePercentage < 0) {
        this.minerFeesToOrdersPercentage += -this.participations[i].order.waiveFeePercentage;
      }
    }
    this.valid = this.valid && ensure(this.minerFeesToOrdersPercentage <= this.context.feePercentageBase,
                                      "miner distributes more than 100% of its fees to order owners");

    // Ring calculations are done. Make sure te remove all reservations for this ring
    for (const p of this.participations) {
      this.orderUtil.resetReservations(p.order);
    }
  }

  public async setMaxFillAmounts(p: Participation) {
    const remainingS = new BigNumber(p.order.amountS).minus(p.order.filledAmountS);
    p.ringSpendableS = await this.orderUtil.getSpendableS(p.order);
    p.fillAmountS = BigNumber.min(p.ringSpendableS, remainingS);

    if (!p.order.P2P) {
      // No need to check the fee balance of the owner if feeToken == tokenB,
      // fillAmountB will be used to pay the fee.
      if (!(p.order.feeToken === p.order.tokenB &&
            p.order.owner === p.order.tokenRecipient &&
            p.order.feeAmount <= p.order.amountB)) {
        // Check how much fee needs to be paid. We limit fillAmountS to how much
        // fee the order owner can pay.
        let feeAmount = new BigNumber(p.order.feeAmount).times(p.fillAmountS).dividedToIntegerBy(p.order.amountS);
        if (feeAmount.gt(0)) {
          const spendableFee = await this.orderUtil.getSpendableFee(p.order);
          if (p.order.feeToken === p.order.tokenS && p.fillAmountS.add(feeAmount).gt(p.ringSpendableS)) {
            assert(spendableFee.eq(p.ringSpendableS), "spendableFee == spendableS when feeToken == tokenS");
            // Equally divide the available tokens between fillAmountS and feeAmount
            const totalAmount = new BigNumber(p.order.amountS).add(p.order.feeAmount);
            p.fillAmountS = p.ringSpendableS.times(p.order.amountS).dividedToIntegerBy(totalAmount);
            feeAmount = p.ringSpendableS.mul(p.order.feeAmount).dividedToIntegerBy(totalAmount);
          } else if (feeAmount.gt(spendableFee)) {
            feeAmount = spendableFee;
            p.fillAmountS = feeAmount.times(p.order.amountS).dividedToIntegerBy(p.order.feeAmount);
          }
        }
      }
    }

    p.fillAmountB = p.fillAmountS.times(p.order.amountB).dividedToIntegerBy(p.order.amountS);
  }

  public async calculateFees(p: Participation, prevP: Participation) {
    if (p.order.P2P) {
      // Calculate P2P fees
      p.feeAmount = new BigNumber(0);
      p.feeAmountS = p.fillAmountS.times(p.order.tokenSFeePercentage)
                                  .dividedToIntegerBy(this.context.feePercentageBase);
      p.feeAmountB = p.fillAmountB.times(p.order.tokenBFeePercentage)
                                  .dividedToIntegerBy(this.context.feePercentageBase);
    } else {
      // Calculate matching fees
      p.feeAmount = new BigNumber(p.order.feeAmount).times(p.fillAmountS).dividedToIntegerBy(p.order.amountS);
      p.feeAmountS = new BigNumber(0);
      p.feeAmountB = new BigNumber(0);

      // If feeToken == tokenB AND owner == tokenRecipient, try to pay using fillAmountB
      if (p.order.feeToken === p.order.tokenB &&
          p.order.owner === p.order.tokenRecipient &&
          p.fillAmountB.gte(p.feeAmount)) {
        p.feeAmountB = p.feeAmount;
        p.feeAmount = new BigNumber(0);
      }

      // Make sure we can pay the feeAmount
      p.ringSpendableFee = await this.orderUtil.getSpendableFee(p.order);
      if (p.feeAmount.gt(p.ringSpendableFee)) {
          // This normally should not happen, but this is possible when self-trading
          return false;
      } else {
        await this.orderUtil.reserveAmountFee(p.order, p.feeAmount);
      }
    }

    if (p.fillAmountS.minus(p.feeAmountS).gte(prevP.fillAmountB)) {
      // The miner (or in a P2P case, the taker) gets the margin
      p.splitS = (p.fillAmountS.minus(p.feeAmountS)).minus(prevP.fillAmountB);
      p.fillAmountS = prevP.fillAmountB.plus(p.feeAmountS);
      return true;
    } else {
      return false;
    }
  }

  public generateFills() {
    const fills: Fill[] = [];
    for (const p of this.participations) {
      let feeAmount = p.feeAmount;
      if (!p.order.P2P) {
        feeAmount = feeAmount.plus(p.feeAmountB);
      }
      const fill: Fill = {
        orderHash: "0x" + p.order.hash.toString("hex"),
        owner: p.order.owner,
        tokenS: p.order.tokenS,
        amountS: p.fillAmountS,
        split: p.splitS,
        feeAmount,
      };
      fills.push(fill);
    }
    return fills;
  }

  public adjustOrderState(p: Participation) {
    // Update filled amount
    p.order.filledAmountS = p.order.filledAmountS.plus(p.fillAmountS).plus(p.splitS);

    // Update spendables
    const totalAmountS = p.fillAmountS.plus(p.splitS);
    const totalAmountFee = p.feeAmount;
    p.order.tokenSpendableS.amount = p.order.tokenSpendableS.amount.minus(totalAmountS);
    p.order.tokenSpendableFee.amount = p.order.tokenSpendableFee.amount.minus(totalAmountFee);
    if (p.order.brokerInterceptor) {
      p.order.brokerSpendableS.amount = p.order.brokerSpendableS.amount.minus(totalAmountS);
      p.order.brokerSpendableFee.amount = p.order.brokerSpendableFee.amount.minus(totalAmountFee);
      assert(p.order.brokerSpendableS.amount.gte(0), "brokerSpendableS should be positive");
      assert(p.order.tokenSpendableFee.amount.gte(0), "tokenSpendableFee should be positive");
    }
    // Checks
    assert(p.order.tokenSpendableS.amount.gte(0), "spendableS should be positive");
    assert(p.order.tokenSpendableFee.amount.gte(0), "spendableFee should be positive");
    assert(p.order.filledAmountS.lte(p.order.amountS), "filledAmountS <= amountS");
  }

  public adjustOrderStates() {
    // Adjust orders
    for (const p of this.participations) {
      this.adjustOrderState(p);
    }
  }

  public revertOrderStats() {
    // Adjust orders
    for (const p of this.participations) {
      p.order.filledAmountS = p.order.filledAmountS.minus(p.fillAmountS.plus(p.splitS));
      assert(p.order.filledAmountS.gte(0), "p.order.filledAmountS >= 0");
    }
  }

  public async doPayments(mining: Mining, feeBalances: BalanceBook) {
    if (!this.valid) {
      return [];
    }

    await this.payFees(mining);
    const transferItems = await this.transferTokens(mining);

    // Validate how the ring is settled
    await this.validateSettlement(mining, transferItems);

    // Add the fee balances to the global fee list
    for (const balance of this.feeBalances.getAllBalances()) {
      feeBalances.addBalance(balance.owner, balance.token, balance.tranche, balance.amount);
    }

    return transferItems;
  }

  private async transferTokens(mining: Mining) {
    const ringSize = this.participations.length;
    const transferItems: TransferItem[] = [];
    for (let i = 0; i < ringSize; i++) {
      const prevIndex = (i + ringSize - 1) % ringSize;
      const p = this.participations[i];
      const prevP = this.participations[prevIndex];
      const feeHolder = this.context.feeHolder.address;

      const buyerFeeAmountAfterRebateB = prevP.feeAmountB.minus(prevP.rebateB);
      assert(buyerFeeAmountAfterRebateB.gte(0), "buyerFeeAmountAfterRebateB >= 0");

      // If the buyer needs to pay fees in a percentage of tokenB, the seller needs
      // to send that amount of tokenS to the fee holder contract.
      const amountSToBuyer = p.fillAmountS
                             .minus(p.feeAmountS)
                             .minus(buyerFeeAmountAfterRebateB);
      let amountSToFeeHolder = (p.feeAmountS.minus(p.rebateS))
                               .plus(buyerFeeAmountAfterRebateB);
      let amountFeeToFeeHolder = p.feeAmount.minus(p.rebateFee);
      if (p.order.tokenS === p.order.feeToken) {
        amountSToFeeHolder = amountSToFeeHolder.plus(amountFeeToFeeHolder);
        amountFeeToFeeHolder = new BigNumber(0);
      }

      let margin = p.splitS;
      // Don't pay out the margin to the miner if it's a security token
      if (p.order.tokenTypeS === TokenType.ERC1400) {
          margin = new BigNumber(0);
      }

      await this.addTokenTransfer(transferItems,
                                  p.order.tokenTypeS,
                                  p.order.trancheS,
                                  p.order.tokenS,
                                  p.order.owner,
                                  prevP.order.tokenRecipient,
                                  amountSToBuyer,
                                  p.order.transferDataS);
      await this.addTokenTransfer(transferItems,
                                  p.order.tokenTypeS,
                                  p.order.trancheS,
                                  p.order.tokenS,
                                  p.order.owner,
                                  feeHolder,
                                  amountSToFeeHolder,
                                  p.order.transferDataS);
      await this.addTokenTransfer(transferItems,
                                  p.order.tokenTypeFee,
                                  p.order.trancheFee,
                                  p.order.feeToken,
                                  p.order.owner,
                                  feeHolder,
                                  amountFeeToFeeHolder,
                                  "0x");
      await this.addTokenTransfer(transferItems,
                                  p.order.tokenTypeS,
                                  p.order.trancheS,
                                  p.order.tokenS,
                                  p.order.owner,
                                  mining.feeRecipient,
                                  margin,
                                  p.order.transferDataS);

      // BEGIN diagnostics
      this.detailTransferS[i].amount = p.fillAmountS.plus(margin).toNumber();
      this.logPayment(this.detailTransferS[i], p.order.tokenS, p.order.owner, prevP.order.tokenRecipient,
                      p.fillAmountS.minus(p.feeAmountS), "ToBuyer");
      this.logPayment(this.detailTransferS[i], p.order.tokenS, p.order.owner, mining.feeRecipient,
                      margin, "Margin");
      this.detailTransferB[i].amount = p.fillAmountB.toNumber();
      this.detailTransferFee[i].amount = p.feeAmount.toNumber();
      // END diagnostics
    }
    return transferItems;
  }

  private async addTokenTransfer(transferItems: TransferItem[],
                                 tokenType: TokenType,
                                 fromTranche: string,
                                 token: string,
                                 from: string,
                                 to: string,
                                 amount: BigNumber,
                                 data: string) {
    if (from !== to && amount.gt(0)) {
      if (tokenType === TokenType.ERC20) {
        transferItems.push({token, from, to, amount, tokenType,
                            fromTranche: this.zeroAddress, toTranche: this.zeroAddress});
      } else if (tokenType === TokenType.ERC1400) {
        const ERC1400token = this.context.ERC1400Contract.at(token);
        const tranferData = data ? data : "0x";
        const [ESC, unused, destTranche] = await ERC1400token.canSend(from, to, fromTranche, amount, tranferData);
        assert(ESC === "0xa0" || ESC === "0xa1" || ESC === "0xa2", "Cannot transfer ERC1400 tokens");
        transferItems.push({token, from, to, amount, tokenType,
                            fromTranche, toTranche: destTranche, data: tranferData});
      }
    }
  }

  private async payFees(mining: Mining) {
    const ringSize = this.participations.length;
    for (let i = 0; i < ringSize; i++) {
      const p = this.participations[i];

      const walletPercentage = p.order.P2P ? 100 :
                               (p.order.walletAddr ? p.order.walletSplitPercentage : 0);

      // Save these fee amounts before any discount the order gets for validation
      const feePercentageB = p.order.P2P ? p.order.tokenBFeePercentage : p.order.feePercentage;

      p.rebateFee = await this.payFeesAndBurn(mining,
                                              p,
                                              p.order.feeToken,
                                              p.feeAmount,
                                              walletPercentage,
                                              this.detailTransferFee[i]);
      p.rebateS = await this.payFeesAndBurn(mining,
                                            p,
                                            p.order.tokenS,
                                            p.feeAmountS,
                                            walletPercentage,
                                            this.detailTransferS[i],
                                            p.order.tokenSFeePercentage);
      p.rebateB = await this.payFeesAndBurn(mining,
                                            p,
                                            p.order.tokenB,
                                            p.feeAmountB,
                                            walletPercentage,
                                            this.detailTransferB[i],
                                            feePercentageB);
    }
  }

  private async payFeesAndBurn(mining: Mining,
                               p: Participation,
                               token: string,
                               totalAmount: BigNumber,
                               walletSplitPercentage: number,
                               payment: DetailedTokenTransfer,
                               feePercentage: number = 0) {
    if (totalAmount.eq(0)) {
      return new BigNumber(0);
    }

    let amount = totalAmount;
    // No need to pay any fees in a P2P order without a wallet
    // (but the fee amount is a part of amountS of the order, so the fee amount is rebated).
    if (p.order.P2P && !p.order.walletAddr) {
      amount = new BigNumber(0);
    }

    // Pay the burn rate with the feeHolder as owner
    const burnAddress = this.context.feeHolder.address;

    // BEGIN diagnostics
    let feeDesc = "Fee";
    if (!p.order.P2P && p.order.feeToken === p.order.tokenB) {
      feeDesc += "@feeToken";
    } else {
      feeDesc += ((feePercentage > 0) ? ("@" + (feePercentage / 10)) + "%" : "");
    }
    const feePayment = this.logPayment(payment, token, p.order.owner, "NA", amount, feeDesc);
    // END diagnostics

    const walletFee = amount.times(walletSplitPercentage).dividedToIntegerBy(100);
    let minerFee = amount.minus(walletFee);

    // BEGIN diagnostics
    const walletPayment = this.logPayment(
      feePayment, token, p.order.owner, "NA", walletFee, "Wallet@" + walletSplitPercentage + "%");
    let minerPayment = this.logPayment(
      feePayment, token, p.order.owner, "NA", minerFee, "Miner@" + (100 - walletSplitPercentage) + "%");
    // END diagnostics

    // Miner can waive fees for this order. If waiveFeePercentage > 0 this is a simple reduction in fees.
    if (p.order.waiveFeePercentage > 0) {
      minerFee = minerFee
                 .times(this.context.feePercentageBase - p.order.waiveFeePercentage)
                 .dividedToIntegerBy(this.context.feePercentageBase);

      // BEGIN diagnostics
      minerPayment = this.logPayment(
        minerPayment, token, p.order.owner, "NA", minerFee, "Waive@" + p.order.waiveFeePercentage / 10 + "%");
      // END diagnostics
    } else if (p.order.waiveFeePercentage < 0) {
      // No fees need to be paid to the miner by this order
      minerFee = new BigNumber(0);

      // BEGIN diagnostics
      minerPayment = this.logPayment(
        minerPayment, token, p.order.owner, "NA", minerFee, "Waive@" + p.order.waiveFeePercentage / 10 + "%");
      // END diagnostics
    }

    // Calculate burn rates and rebates
    const burnRateToken = (await this.context.burnRateTable.getBurnRate(token)).toNumber();
    const burnRate = p.order.P2P ? (burnRateToken >> 16) : (burnRateToken & 0xFFFF);
    const rebateRate = 0;
    // Miner fee
    const minerBurn = minerFee.times(burnRate).dividedToIntegerBy(this.context.feePercentageBase);
    const minerRebate = minerFee.times(rebateRate).dividedToIntegerBy(this.context.feePercentageBase);
    minerFee = minerFee.minus(minerBurn).minus(minerRebate);
    // Wallet fee
    const walletBurn = walletFee.times(burnRate).dividedToIntegerBy(this.context.feePercentageBase);
    const walletRebate = walletFee.times(rebateRate).dividedToIntegerBy(this.context.feePercentageBase);
    const feeToWallet = walletFee.minus(walletBurn).minus(walletRebate);

    // BEGIN diagnostics
    this.logPayment(minerPayment, token, p.order.owner, burnAddress, minerBurn, "Burn@" + burnRate / 10 + "%");
    this.logPayment(minerPayment, token, p.order.owner, p.order.owner, minerRebate, "Rebate@" + rebateRate / 10 + "%");
    const minerIncomePayment =
      this.logPayment(minerPayment, token, p.order.owner, mining.feeRecipient, minerFee, "Income");
    this.logPayment(walletPayment, token, p.order.owner, burnAddress, walletBurn, "Burn@" + burnRate / 10 + "%");
    this.logPayment(walletPayment, token, p.order.owner, p.order.owner, walletRebate,
                    "Rebate@" + rebateRate / 10 + "%");
    this.logPayment(walletPayment, token, p.order.owner, p.order.walletAddr, feeToWallet, "Income");
    // END diagnostics

    // Fees can be paid out in different tokens so we can't easily accumulate the total fee
    // that needs to be paid out to order owners. So we pay out each part out here to all orders that need it.
    let feeToMiner = minerFee;
    if (this.minerFeesToOrdersPercentage > 0 && minerFee.gt(0)) {
      // Pay out the fees to the orders
      for (const otherP of this.participations) {
        if (otherP.order.waiveFeePercentage < 0) {
          const feeToOwner = minerFee
                             .times(-otherP.order.waiveFeePercentage)
                             .dividedToIntegerBy(this.context.feePercentageBase);
          await this.addFeePayment(token, otherP.order.owner, feeToOwner);

          // BEGIN diagnostics
          this.logPayment(minerIncomePayment, token, p.order.owner, otherP.order.owner, feeToOwner,
            "Share_Income@" + (-otherP.order.waiveFeePercentage) / 10 + "%");
          // END diagnostics
        }
      }
      // Subtract all fees the miner pays to the orders
      feeToMiner = minerFee
                   .times(this.context.feePercentageBase - this.minerFeesToOrdersPercentage)
                   .dividedToIntegerBy(this.context.feePercentageBase);
    }

    // Do the fee payments
    await this.addFeePayment(token, p.order.walletAddr, feeToWallet);
    await this.addFeePayment(token, mining.feeRecipient, feeToMiner);
    // Burn
    await this.addFeePayment(token, burnAddress, minerBurn.plus(walletBurn));

    // Calculate the total fee payment after possible discounts (burn rate rebate + fee waiving)
    const totalFeePaid = (feeToWallet.plus(minerFee)).plus(minerBurn.plus(walletBurn));
    assert(totalFeePaid.lte(totalAmount), "Total fee paid cannot exceed the total fee amount");

    // Return the rebate this order got
    return totalAmount.minus(totalFeePaid);
  }

  private async addFeePayment(token: string,
                              to: string,
                              amount: BigNumber) {
    if (!token || !to || !amount) {
      return;
    }
    this.feeBalances.addBalance(to, token, this.zeroAddress, amount);
  }

  private resize(i: number, smallest: number) {
    let newSmallest = smallest;
    const j = (i + this.participations.length - 1) % this.participations.length;
    const p = this.participations[i];
    const prevP = this.participations[j];

    let postFeeFillAmountS = p.fillAmountS;
    if (p.order.tokenSFeePercentage > 0) {
      const feeAmountS = p.fillAmountS.times(p.order.tokenSFeePercentage)
                                      .dividedToIntegerBy(this.context.feePercentageBase);
      postFeeFillAmountS = p.fillAmountS.minus(feeAmountS);
    }
    if (prevP.fillAmountB.gt(postFeeFillAmountS)) {
      newSmallest = i;
      prevP.fillAmountB = postFeeFillAmountS;
      prevP.fillAmountS = prevP.fillAmountB.times(prevP.order.amountS).dividedToIntegerBy(prevP.order.amountB);
    }

    return newSmallest;
  }

  private async validateSettlement(mining: Mining, transfers: TransferItem[]) {
    const expectedTotalBurned = new BalanceBook();
    const feeAddress = this.context.feeHolder.address;
    const ringSize = this.participations.length;
    for (let i = 0; i < ringSize; i++) {
      const prevIndex = (i + ringSize - 1) % ringSize;
      const p = this.participations[i];
      const prevP = this.participations[prevIndex];

      logDebug("p.spendableS:           " + p.ringSpendableS.toNumber() / 1e18);
      logDebug("p.spendableFee:         " + p.ringSpendableFee.toNumber() / 1e18);
      logDebug("order.amountS:          " + p.order.amountS / 1e18);
      logDebug("order.amountB:          " + p.order.amountB / 1e18);
      logDebug("order.feeAmount:        " + p.order.feeAmount / 1e18);
      logDebug("order expected rate:    " + p.order.amountS / p.order.amountB);
      logDebug("p.fillAmountS:          " + p.fillAmountS.toNumber() / 1e18);
      logDebug("p.fillAmountB:          " + p.fillAmountB.toNumber() / 1e18);
      logDebug("p.splitS:               " + p.splitS.toNumber() / 1e18);
      logDebug("order actual rate:      " + p.fillAmountS.plus(p.splitS).div(p.fillAmountB).toNumber());
      logDebug("p.feeAmount:            " + p.feeAmount.toNumber() / 1e18);
      logDebug("p.feeAmountS:           " + p.feeAmountS.toNumber() / 1e18);
      logDebug("p.feeAmountB:           " + p.feeAmountB.toNumber() / 1e18);
      logDebug("p.rebateFee:            " + p.rebateFee.toNumber() / 1e18);
      logDebug("p.rebateS:              " + p.rebateS.toNumber() / 1e18);
      logDebug("p.rebateB:              " + p.rebateB.toNumber() / 1e18);
      logDebug("tokenS percentage:      " + (p.order.P2P ? p.order.tokenSFeePercentage : 0) /
                                               this.context.feePercentageBase);
      logDebug("tokenS real percentage: " + p.feeAmountS.toNumber() / p.order.amountS);
      logDebug("tokenB percentage:      " +
        (p.order.P2P ? p.order.tokenBFeePercentage : p.order.feePercentage) / this.context.feePercentageBase);
      logDebug("tokenB real percentage: " + p.feeAmountB.toNumber() / p.fillAmountB.toNumber());
      logDebug("----------------------------------------------");

      // Sanity checks
      assert(p.fillAmountS.gte(0), "fillAmountS should be positive");
      assert(p.splitS.gte(0), "splitS should be positive");
      assert(p.feeAmount.gte(0), "feeAmount should be positive");
      assert(p.feeAmountS.gte(0), "feeAmountS should be positive");
      assert(p.feeAmountB.gte(0), "feeAmountB should be positive");
      assert(p.rebateFee.gte(0), "rebateFee should be positive");
      assert(p.rebateS.gte(0), "rebateFeeS should be positive");
      assert(p.rebateB.gte(0), "rebateFeeB should be positive");

      // General fill requirements
      assert(p.fillAmountS.plus(p.splitS).lte(p.order.amountS), "fillAmountS + splitS <= amountS");
      assert(p.fillAmountB.lte(p.order.amountB), "fillAmountB <= amountB");
      assert(p.feeAmount.lte(p.order.feeAmount), "fillAmountFee <= feeAmount");
      if (p.fillAmountS.gt(0) || p.fillAmountB.gt(0)) {
        const orderRate = p.order.amountS / p.order.amountB;
        const rate = p.fillAmountS.plus(p.splitS).div(p.fillAmountB).toNumber();
        this.assertAlmostEqual(rate, orderRate, "fill rates need to match order rate");
      }
      assert(p.rebateFee.lte(p.feeAmount), "p.rebateFee <= p.feeAmount");
      assert(p.rebateS.lte(p.feeAmountS), "p.rebateS <= p.feeAmountS");
      assert(p.rebateB.lte(p.feeAmountB), "p.rebateB <= p.feeAmountB");

      // Miner/Taker gets all margin
      assert(p.fillAmountS.minus(p.feeAmountS).eq(prevP.fillAmountB), "fillAmountS == prev.fillAmountB");

      // Spendable limitations
      {
        const totalAmountTokenS = p.fillAmountS.plus(p.splitS);
        const totalAmountTokenFee = p.feeAmount;
        if (p.order.tokenS === p.order.feeToken) {
          assert(totalAmountTokenS.plus(totalAmountTokenFee).lte(p.ringSpendableS),
                 "totalAmountTokenS + totalAmountTokenFee <= spendableS");
        } else {
          assert(totalAmountTokenS.lte(p.ringSpendableS), "totalAmountTokenS <= spendableS");
          assert(totalAmountTokenFee.lte(p.ringSpendableFee ? p.ringSpendableFee : new BigNumber(0)),
                 "totalAmountTokenFee <= spendableFee");
        }
      }

      // Ensure fees are calculated correctly
      if (p.order.P2P) {
        // Fee cannot be paid in tokenFee
        assert(p.feeAmount.isZero(), "Cannot pay in tokenFee in a P2P order");
        // Check if fees were calculated correctly for the expected rate
        if (p.order.walletAddr) {
          // fees in tokenS
          {
            const rate = p.feeAmountS.div(p.fillAmountS).toNumber();
            this.assertAlmostEqual(rate, p.order.tokenSFeePercentage,
                                   "tokenS fee rate needs to match given rate");
          }
          // fees in tokenB
          {
            const rate = p.feeAmountB.div(p.fillAmountB).toNumber();
            this.assertAlmostEqual(rate, p.order.tokenBFeePercentage,
                                   "tokenB fee rate needs to match given rate");
          }
        } else {
          // No fees need to be paid when no wallet is given
          assert(p.feeAmountS.eq(p.rebateS), "No fees need to paid without wallet in a P2P order");
          assert(p.feeAmountB.eq(p.rebateB), "No fees need to paid without wallet in a P2P order");
        }
      } else {
        // Fee cannot be paid in tokenS
        assert(p.feeAmountS.isZero(), "Cannot pay in tokenS");
        // Fees need to be paid either in feeToken OR tokenB, never both at the same time
        assert(!(p.feeAmount.gt(0) && p.feeAmountB.gt(0)), "fees should be paid in tokenFee OR tokenB");

        const fee = new BigNumber(p.order.feeAmount).times(p.fillAmountS.plus(p.splitS))
                                                    .dividedToIntegerBy(p.order.amountS);

        // Fee can only be paid in tokenB when feeToken == tokenB
        if (p.order.feeToken === p.order.tokenB && p.order.owner === p.order.tokenRecipient && p.fillAmountB.gte(fee)) {
          assert(p.feeAmountB.eq(fee), "Fee should be paid in tokenB using feeAmount");
          assert(p.feeAmount.isZero(), "Fee should not be paid in feeToken");
        } else {
          assert(p.feeAmountB.isZero(), "Fee should not be paid in tokenB");
          assert(p.feeAmount.eq(fee), "Fee should be paid in feeToken using feeAmount");
          assert(p.feeAmount.lte(p.ringSpendableFee), "Fee <= spendableFee");
        }
      }

      // Check rebates and burn rates
      const calculateBurnAndRebate = async (token: string, amount: BigNumber) => {
        const walletSplitPercentage = p.order.P2P ? 100 : p.order.walletSplitPercentage;
        const walletFee = amount.times(walletSplitPercentage).dividedToIntegerBy(100);
        let minerFee = amount.minus(walletFee);
        if (p.order.waiveFeePercentage > 0) {
          minerFee = minerFee
                     .times(this.context.feePercentageBase - p.order.waiveFeePercentage)
                     .dividedToIntegerBy(this.context.feePercentageBase);
        } else if (p.order.waiveFeePercentage < 0) {
          // No fees need to be paid to the miner by this order
          minerFee = new BigNumber(0);
        }
        // Calculate burn rates and rebates
        const burnRateToken = (await this.context.burnRateTable.getBurnRate(token)).toNumber();
        const burnRate = p.order.P2P ? (burnRateToken >> 16) : (burnRateToken & 0xFFFF);
        const rebateRate = 0;
        // Miner fee
        const minerBurn = minerFee.times(burnRate).dividedToIntegerBy(this.context.feePercentageBase);
        const minerRebate = minerFee.times(rebateRate).dividedToIntegerBy(this.context.feePercentageBase);
        minerFee = minerFee.minus(minerBurn).minus(minerRebate);
        // Wallet fee
        const walletBurn = walletFee.times(burnRate).dividedToIntegerBy(this.context.feePercentageBase);
        const walletRebate = walletFee.times(rebateRate).dividedToIntegerBy(this.context.feePercentageBase);
        const feeToWallet = walletFee.minus(walletBurn).minus(walletRebate);
        const totalFeePaid = (feeToWallet.plus(minerFee)).plus(minerBurn.plus(walletBurn));
        // Return the rebate this order got
        return [minerBurn.plus(walletBurn), amount.minus(totalFeePaid)];
      };
      const [expectedBurnFee, expectedRebateFee] = await calculateBurnAndRebate(p.order.feeToken, p.feeAmount);
      let [expectedBurnS, expectedRebateS] = await calculateBurnAndRebate(p.order.tokenS, p.feeAmountS);
      let [expectedBurnB, expectedRebateB] = await calculateBurnAndRebate(p.order.tokenB, p.feeAmountB);

      if (p.order.P2P && !p.order.walletAddr) {
        expectedRebateS = p.feeAmountS;
        expectedBurnS = new BigNumber(0);
        expectedRebateB = p.feeAmountB;
        expectedBurnB = new BigNumber(0);
      }

      assert(p.rebateFee.eq(expectedRebateFee), "Fee rebate should match expected value");
      assert(p.rebateS.eq(expectedRebateS), "FeeS rebate should match expected value");
      assert(p.rebateB.eq(expectedRebateB), "FeeB rebate should match expected value");

      // Add burn rates to total expected burn rates
      expectedTotalBurned.addBalance(this.zeroAddress, p.order.feeToken, this.zeroAddress, expectedBurnFee);
      expectedTotalBurned.addBalance(this.zeroAddress, p.order.tokenS, p.order.trancheS, expectedBurnS);
      expectedTotalBurned.addBalance(this.zeroAddress, p.order.tokenB, p.order.trancheB, expectedBurnB);

      // Ensure fees in tokenB can be paid with the amount bought
      assert(prevP.feeAmountB.lte(p.fillAmountS), "Can't pay more in tokenB fees than what was bought");
    }

    // Ensure balances are updated correctly
    // Simulate the token transfers in the ring
    const balances = new BalanceBook();
    for (const transfer of transfers) {
      balances.addBalance(transfer.from, transfer.token, transfer.fromTranche, transfer.amount.neg());
      balances.addBalance(transfer.to, transfer.token, transfer.toTranche, transfer.amount);
    }
    // Accumulate owner balances and accumulate fees
    const expectedBalances = new BalanceBook();
    const expectedFeeBalances = new BalanceBook();
    for (let i = 0; i < ringSize; i++) {
      const p = this.participations[i];
      const nextP = this.participations[(i + 1) % ringSize];
      // Owner balances
      const expectedBalanceS = p.fillAmountS.minus(p.rebateS).plus(p.splitS).negated();
      const expectedBalanceB = p.fillAmountB.minus(p.feeAmountB.minus(p.rebateB));
      const expectedBalanceFeeToken = p.feeAmount.minus(p.rebateFee).negated();

      // Accumulate balances
      expectedBalances.addBalance(p.order.owner, p.order.tokenS, p.order.trancheS, expectedBalanceS);
      expectedBalances.addBalance(p.order.tokenRecipient, p.order.tokenB, p.order.trancheB, expectedBalanceB);
      expectedBalances.addBalance(p.order.owner, p.order.feeToken, this.zeroAddress, expectedBalanceFeeToken);
      expectedBalances.addBalance(mining.feeRecipient, p.order.tokenS, p.order.trancheS, p.splitS);

      // Accumulate fees
      expectedFeeBalances.addBalance(feeAddress, p.order.tokenS, p.order.trancheS, p.feeAmountS.minus(p.rebateS));
      expectedFeeBalances.addBalance(feeAddress, p.order.tokenB, p.order.trancheB, p.feeAmountB.minus(p.rebateB));
      expectedFeeBalances.addBalance(feeAddress, p.order.feeToken, this.zeroAddress, p.feeAmount.minus(p.rebateFee));
    }
    // Check balances of all owners
    for (let i = 0; i < ringSize; i++) {
      const p = this.participations[i];

      const balanceS = balances.getBalance(p.order.owner, p.order.tokenS, p.order.trancheS);
      const balanceB = balances.getBalance(p.order.tokenRecipient, p.order.tokenB, p.order.trancheB);
      const balanceFee = balances.getBalance(p.order.owner, p.order.feeToken, this.zeroAddress);

      const expectedBalanceS = expectedBalances.getBalance(p.order.owner, p.order.tokenS, p.order.trancheS);
      const expectedBalanceB = expectedBalances.getBalance(p.order.tokenRecipient, p.order.tokenB, p.order.trancheB);
      const expectedBalanceFee = expectedBalances.getBalance(p.order.owner, p.order.feeToken, this.zeroAddress);

      assert(balanceS.eq(expectedBalanceS), "Order owner tokenS balance should match expected value");
      assert(balanceB.eq(expectedBalanceB), "Order tokenRecipient tokenB balance should match expected value");
      assert(balanceFee.eq(expectedBalanceFee), "Order owner feeToken balance should match expected value");
    }
    // Check fee holder balances of all possible tokens used to pay fees
    for (const token of [...Object.keys(expectedFeeBalances), ...balances.getAllTokens()]) {
      const expectedBalance = expectedFeeBalances.getBalance(feeAddress, token, this.zeroAddress);
      const balance = balances.getBalance(feeAddress, token, this.zeroAddress);
      assert(balance.eq(expectedBalance),
             "FeeHolder balance after transfers should match expected value");
    }

    // Ensure total fee payments match transferred amounts to feeHolder contract
    {
      for (const token of [...Object.keys(this.feeBalances.getAllTokens()), ...Object.keys(balances.getAllTokens())]) {
        let totalFee = new BigNumber(0);
        for (const balance of this.feeBalances.getAllBalances()) {
          if (balance.token === token) {
            totalFee = totalFee.plus(balance.amount);
          }
        }
        const feeHolderBalance = balances.getBalance(feeAddress, token, this.zeroAddress);
        assert(totalFee.eq(feeHolderBalance), "Total fee amount in feeHolder should match total amount transferred");
      }
    }

    // Ensure total burn payments match expected total burned
    const burnTokens = [...Object.keys(expectedTotalBurned.getAllTokens()),
                        ...Object.keys(this.feeBalances.getAllTokens())];
    for (const token of burnTokens) {
      const burned = this.feeBalances.getBalance(this.zeroAddress, token, this.zeroAddress);
      const expected = expectedTotalBurned.getBalance(this.zeroAddress, token, this.zeroAddress);
      assert(burned.eq(expected), "Total burned should match expected value");
    }
  }

  private assertAlmostEqual(n1: number, n2: number, description: string, precision: number = 8) {
    const numStr1 = (n1 / 1e18).toFixed(precision);
    const numStr2 = (n2 / 1e18).toFixed(precision);
    return assert.equal(Number(numStr1), Number(numStr2), description);
  }

  private logPayment(parent: DetailedTokenTransfer,
                     token: string,
                     from: string,
                     to: string,
                     amount: BigNumber,
                     description: string = "NA") {
    const payment: DetailedTokenTransfer = {
      description,
      token,
      from,
      to,
      amount: amount.toNumber(),
      subPayments: [],
    };
    parent.subPayments.push(payment);
    return payment;
  }
}
