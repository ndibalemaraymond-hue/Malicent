import uuid
import datetime
import threading
import logging
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TransactionStatus(Enum):
    PENDING = "PENDING"
    SWEPT = "SWEPT"
    FAILED = "FAILED"
    SKIPPED_INSUFFICIENT_BALANCE = "SKIPPED_INSUFFICIENT_BALANCE"


@dataclass
class Transaction:
    tx_id: str
    user_id: str
    amount: Decimal
    timestamp: datetime.datetime = field(default_factory=datetime.datetime.now)
    status: TransactionStatus = TransactionStatus.PENDING
    sweep_amount: Decimal = Decimal("0")
    fee: Decimal = Decimal("0")
    error_message: Optional[str] = None


@dataclass
class InvestmentRecord:
    bond_id: str
    amount: Decimal
    timestamp: str
    status: str


class MalicentEngine:
    MINIMUM_BALANCE_THRESHOLD = Decimal("5000")
    VAULT_THRESHOLD = Decimal("1000000")
    SWEEP_ROUNDING = Decimal("100")
    
    MUGAMARU_PERCENT = Decimal("0.10")  # 10% of sweep goes to Mugamaru
    DSE_INTEREST_RATE = Decimal("0.12")   # 12% annual interest from DSE bonds

    def __init__(self, 
                 vault_threshold: Decimal = None,
                 minimum_balance: Decimal = None):
        self.vault_threshold = vault_threshold or self.VAULT_THRESHOLD
        self.minimum_balance = minimum_balance or self.MINIMUM_BALANCE_THRESHOLD
        
        self._vault_balance = Decimal("0")
        self._fractional_ledger: dict[str, Decimal] = {}
        self._investment_history: list[InvestmentRecord] = []
        
        self._mugamaru_account = Decimal("0")
        self._dse_interest_earned = Decimal("0")
        self._total_dividends_distributed = Decimal("0")
        
        self._processed_tx_ids: set[str] = set()
        self._lock = threading.RLock()

    @property
    def current_vault_balance(self) -> Decimal:
        return self._vault_balance

    @property
    def fractional_ledger(self) -> dict:
        return dict(self._fractional_ledger)

    @property
    def transaction_fee_pool(self) -> Decimal:
        return self._mugamaru_account

    @property
    def mugamaru_account(self) -> Decimal:
        return self._mugamaru_account
    
    @property
    def dse_interest_earned(self) -> Decimal:
        return self._dse_interest_earned
    
    @property
    def total_dividends_distributed(self) -> Decimal:
        return self._total_dividends_distributed

    @property
    def investment_history(self) -> list:
        return [vars(r) for r in self._investment_history]

    def get_user_balance(self, user_id: str) -> Decimal:
        return self._fractional_ledger.get(user_id, Decimal("0"))

    def process_transaction(self, 
                            user_id: str, 
                            amount: Decimal | float | int,
                            tx_id: Optional[str] = None) -> Transaction:
        tx_id = tx_id or str(uuid.uuid4())
        
        with self._lock:
            if tx_id in self._processed_tx_ids:
                logger.warning(f"Duplicate transaction detected: {tx_id}")
                return Transaction(
                    tx_id=tx_id,
                    user_id=user_id,
                    amount=Decimal(str(amount)),
                    status=TransactionStatus.SWEPT,
                    error_message="Duplicate transaction"
                )

            amount = Decimal(str(amount))
            
            if amount <= 0:
                logger.error(f"Invalid amount: {amount}")
                return Transaction(
                    tx_id=tx_id,
                    user_id=user_id,
                    amount=amount,
                    status=TransactionStatus.FAILED,
                    error_message="Invalid amount"
                )

            user_fractional_balance = self.get_user_balance(user_id)
            if user_fractional_balance < self.minimum_balance:
                logger.info(f"User {user_id} balance {user_fractional_balance} below threshold {self.minimum_balance} - skipping sweep")
                return Transaction(
                    tx_id=tx_id,
                    user_id=user_id,
                    amount=amount,
                    status=TransactionStatus.SKIPPED_INSUFFICIENT_BALANCE,
                    error_message=f"Balance below minimum ({self.minimum_balance})"
                )

            sweep_amount = self._calculate_sweep(amount)
            
            if sweep_amount <= 0:
                return Transaction(
                    tx_id=tx_id,
                    user_id=user_id,
                    amount=amount,
                    status=TransactionStatus.SWEPT,
                    sweep_amount=Decimal("0")
                )

            return self._execute_sweep(tx_id, user_id, amount, sweep_amount)

    def _calculate_sweep(self, amount: Decimal) -> Decimal:
        remainder = amount % self.SWEEP_ROUNDING
        if remainder == 0:
            return Decimal("0")
        return self.SWEEP_ROUNDING - remainder

    def _execute_sweep(self, tx_id: str, user_id: str, amount: Decimal, sweep_amount: Decimal) -> Transaction:
        mugamaru_share = sweep_amount * self.MUGAMARU_PERCENT
        vault_investment = sweep_amount - mugamaru_share
        
        self._mugamaru_account += mugamaru_share
        
        if user_id not in self._fractional_ledger:
            self._fractional_ledger[user_id] = Decimal("0")
        self._fractional_ledger[user_id] += vault_investment

        self._vault_balance += vault_investment
        
        logger.info(f"Swept TZS {sweep_amount} for User {user_id}. Mugamaru: {mugamaru_share}. To Vault: {vault_investment}. Vault: {self._vault_balance}")

        if self._vault_balance >= self.vault_threshold:
            self._trigger_dse_bond_purchase()

        self._processed_tx_ids.add(tx_id)
        
        return Transaction(
            tx_id=tx_id,
            user_id=user_id,
            amount=amount,
            status=TransactionStatus.SWEPT,
            sweep_amount=sweep_amount,
            fee=mugamaru_share
        )

    def _trigger_dse_bond_purchase(self):
        while self._vault_balance >= self.vault_threshold:
            bond_id = str(uuid.uuid4())[:8]
            purchase_amount = self.vault_threshold
            self._vault_balance -= purchase_amount
            
            investment_record = InvestmentRecord(
                bond_id=f"GOVT-BOND-{bond_id}",
                amount=purchase_amount,
                timestamp=datetime.datetime.now().isoformat(),
                status="SETTLED_AT_DSE"
            )
            self._investment_history.append(investment_record)
            
            interest = purchase_amount * (self.DSE_INTEREST_RATE / Decimal("4"))
            self._dse_interest_earned += interest
            
            logger.critical(f"THRESHOLD REACHED: Purchasing TZS {purchase_amount} Bond {investment_record.bond_id} at DSE. Est. Quarterly Interest: TZS {interest}")

    def distribute_dividends(self) -> dict:
        total_invested = sum(i.amount for i in self._investment_history)
        if total_invested == 0:
            return {"success": False, "message": "No investments to distribute dividends from"}
        
        quarterly_interest = total_invested * (self.DSE_INTEREST_RATE / Decimal("4"))
        
        dividends = {}
        for user, balance in self._fractional_ledger.items():
            if balance > 0:
                user_share = (balance / total_invested) * quarterly_interest
                self._fractional_ledger[user] += user_share
                dividends[user] = user_share
        
        self._total_dividends_distributed += quarterly_interest
        
        logger.info(f"Dividends distributed: TZS {quarterly_interest} to {len(dividends)} users")
        return {"success": True, "total_dividends": float(quarterly_interest), "recipients": len(dividends)}

    def trigger_manual_bond_purchase(self, amount: Decimal = None) -> dict:
        amount = amount or self.vault_threshold
        if self._vault_balance < amount:
            return {"success": False, "message": f"Insufficient vault balance. Have: {self._vault_balance}, Need: {amount}"}
        
        bond_id = str(uuid.uuid4())[:8]
        self._vault_balance -= amount
        
        investment_record = InvestmentRecord(
            bond_id=f"GOVT-BOND-{bond_id}",
            amount=amount,
            timestamp=datetime.datetime.now().isoformat(),
            status="MANUAL_PURCHASE"
        )
        self._investment_history.append(investment_record)
        
        logger.critical(f"MANUAL PURCHASE: TZS {amount} Bond {investment_record.bond_id} at DSE")
        return {"success": True, "bond": vars(investment_record)}

    def get_system_summary(self) -> dict:
        total_invested = sum(i.amount for i in self._investment_history)
        return {
            "vault_balance": float(self._vault_balance),
            "mugamaru_account": float(self._mugamaru_account),
            "dse_interest_earned": float(self._dse_interest_earned),
            "total_dividends_distributed": float(self._total_dividends_distributed),
            "total_bonds_purchased": len(self._investment_history),
            "total_invested": float(total_invested),
            "active_users": len(self._fractional_ledger),
            "vault_threshold": float(self.vault_threshold),
            "threshold_progress": float(self._vault_balance / self.vault_threshold * 100)
        }

    def simulate_bond_accumulation(self, num_transactions: int = 100, avg_amount: Decimal = Decimal("5000")) -> dict:
        import random
        users = [f"USER_{str(i).zfill(3)}" for i in range(1, 21)]
        
        for user in users:
            self._fractional_ledger[user] = Decimal("10000")
        
        for i in range(num_transactions):
            user = random.choice(users)
            amount = Decimal(str(random.randint(int(float(avg_amount) * 0.5), int(float(avg_amount) * 1.5))))
            tx_id = f"SIM_{str(i).zfill(4)}"
            self.process_transaction(user, amount, tx_id)
        
        return self.get_system_summary()

    def get_portfolio_summary(self, user_id: str) -> dict:
        balance = self.get_user_balance(user_id)
        return {
            "user_id": user_id,
            "fractional_balance": float(balance),
            "ownership_percentage": float(balance / self.vault_threshold * 100) if self.vault_threshold > 0 else 0,
            "total_investments": len([i for i in self._investment_history])
        }


def demo():
    print("""
============================================================
  MALICENT: Aggregating Micro-Sweeps for DSE Bond Purchase
============================================================
  DSE Minimum Bond Purchase: TZS 1,000,000
  Problem: Individual mobile users cannot afford this
  Solution: MALICENT aggregates micro-sweeps from millions of users
============================================================
    """)
    
    engine = MalicentEngine(vault_threshold=Decimal("1000000"))

    engine._fractional_ledger["USER_001"] = Decimal("10000")
    engine._fractional_ledger["USER_002"] = Decimal("8000")
    engine._fractional_ledger["USER_003"] = Decimal("6000")

    transactions = [
        {"user": "USER_001", "amount": 1820, "tx_id": "TX001"},
        {"user": "USER_002", "amount": 5010, "tx_id": "TX002"},
        {"user": "USER_001", "amount": 950, "tx_id": "TX003"},
        {"user": "USER_003", "amount": 12040, "tx_id": "TX004"},
        {"user": "USER_001", "amount": 1820, "tx_id": "TX001"},
        {"user": "USER_004", "amount": 100, "tx_id": "TX005"},
    ]

    print("--- MALICENT LIVE PROCESSING ---")
    for tx in transactions:
        result = engine.process_transaction(tx['user'], tx['amount'], tx['tx_id'])
        print(f"Tx: {tx['tx_id']} | User: {tx['user']} | Spent: {tx['amount']} | Swept: {result.sweep_amount} | Status: {result.status.value}")

    print("\n--- FINAL LEDGER STATE ---")
    print(f"Fractional Balances: {engine.fractional_ledger}")
    print(f"Total Fees Collected: {engine.transaction_fee_pool}")
    print(f"Bonds Held: {len(engine.investment_history)}")

    print("\n--- PORTFOLIO SUMMARIES ---")
    for user in ["USER_001", "USER_002", "USER_003", "USER_004"]:
        print(f"{user}: {engine.get_portfolio_summary(user)}")


def demo_large_scale():
    print("""
============================================================
  LARGE-SCALE SIMULATION: Demonstrating the MALICENT Model
============================================================
  Goal: Show how TZS 1-100 sweeps from millions of users
        accumulate to reach DSE's TZS 1,000,000 minimum
============================================================
    """)
    
    engine = MalicentEngine(vault_threshold=Decimal("1000000"))
    
    print(f"DSE Minimum Bond Purchase: TZS {engine.vault_threshold:,}")
    print(f"Running simulation with 10,000 users, 100,000 transactions...\n")
    
    import random
    users = [f"USER_{str(i).zfill(5)}" for i in range(1, 10001)]
    
    for user in users:
        engine._fractional_ledger[user] = Decimal(str(random.randint(5000, 50000)))
    
    tx_count = 0
    for i in range(100000):
        user = random.choice(users)
        amount = Decimal(str(random.randint(100, 10000)))
        tx_id = f"SIM_{str(i).zfill(6)}"
        engine.process_transaction(user, amount, tx_id)
        tx_count += 1
        
        if len(engine.investment_history) > 0 and len(engine.investment_history) % 10 == 0:
            print(f"Bonds purchased so far: {len(engine.investment_history)} | Vault: TZS {engine.current_vault_balance:,.0f}")
            break
    
    print(f"\n--- SIMULATION RESULTS ---")
    summary = engine.get_system_summary()
    print(f"Total Transactions: {tx_count:,}")
    print(f"Active Users: {summary['active_users']:,}")
    print(f"Vault Balance: TZS {summary['vault_balance']:,.0f}")
    print(f"Total Fees Collected: TZS {summary['total_fees_collected']:,.0f}")
    print(f"Bonds Purchased: {summary['total_bonds_purchased']}")
    print(f"Total Invested at DSE: TZS {summary['total_invested']:,.0f}")
    print(f"Threshold Progress: {summary['threshold_progress']:.2f}%")
    
    print("\n--- HOW FRACTIONAL OWNERSHIP WORKS ---")
    sample_users = ["USER_00001", "USER_00002", "USER_00003"]
    for u in sample_users:
        bal = engine.get_user_balance(u)
        pct = float(bal / Decimal("1000000") * 100) if summary['total_invested'] > 0 else 0
        print(f"  {u}: TZS {bal:,.0f} = {pct:.6f}% of bond")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "scale":
        demo_large_scale()
    else:
        demo()

