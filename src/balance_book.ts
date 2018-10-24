import { BigNumber } from "bignumber.js";

export interface Balance {
  token: string;
  tranche: string;
  owner: string;
  amount: BigNumber;
}

export class BalanceBook {

  private balances: { [id: string]: any; } = {};

  private zeroAddress = "0x" + "0".repeat(64);

  public getBalance(owner: string, token: string, tranche: string = this.zeroAddress) {
    if (this.isBalanceKnown(owner, token, tranche)) {
        return this.balances[owner][token][tranche];
    } else {
        return new BigNumber(0);
    }
  }

  public addBalance(owner: string, token: string, tranche: string, amount: BigNumber) {
    assert(owner !== undefined);
    assert(token !== undefined);
    assert(tranche !== undefined);
    if (!this.balances[owner]) {
      this.balances[owner] = {};
    }
    if (!this.balances[owner][token]) {
      this.balances[owner][token] = {};
    }
    if (!this.balances[owner][token][tranche]) {
      this.balances[owner][token][tranche] = new BigNumber(0);
    }
    this.balances[owner][token][tranche] = this.balances[owner][token][tranche].plus(amount);
  }

  public isBalanceKnown(owner: string, token: string, tranche: string) {
    return (this.balances[owner] && this.balances[owner][token] && this.balances[owner][token][tranche]);
  }

  public copy() {
    const balanceBook = new BalanceBook();
    for (const owner of Object.keys(this.balances)) {
      for (const token of Object.keys(this.balances[owner])) {
        for (const tranche of Object.keys(this.balances[owner][token])) {
          balanceBook.addBalance(owner, token, tranche, this.balances[owner][token][tranche]);
        }
      }
    }
    return balanceBook;
  }

  public getData() {
    return this.balances;
  }

  public getAllTokens() {
    const tokens = [];
    for (const owner of Object.keys(this.balances)) {
      for (const token of Object.keys(this.balances[owner])) {
        tokens.push(token);
      }
    }
    return tokens;
  }

  public getAllBalances() {
    const balanceList: Balance[] = [];
    for (const owner of Object.keys(this.balances)) {
      for (const token of Object.keys(this.balances[owner])) {
        for (const tranche of Object.keys(this.balances[owner][token])) {
          const balanceItem: Balance = {
            owner,
            token,
            tranche,
            amount: this.balances[owner][token][tranche],
          };
          balanceList.push(balanceItem);
        }
      }
    }
    return balanceList;
  }

}
