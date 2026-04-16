import uuid
import datetime
import logging
from decimal import Decimal
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

    MUGAMARU_PERCENT = Decimal("0.10")
    DSE_INTEREST_RATE = Decimal("0.12")

    def __init__(self,
                 vault_threshold: Decimal | str | int | None = None,
                 minimum_balance: Decimal | str | int | None = None):
        self.vault_threshold = self._to_decimal(vault_threshold) if vault_threshold is not None else self.VAULT_THRESHOLD
        self.minimum_balance = self._to_decimal(minimum_balance) if minimum_balance is not None else self.MINIMUM_BALANCE_THRESHOLD

        self._vault_balance = Decimal("0")
        self._fractional_ledger: dict[str, Decimal] = {}
        self._investment_history: list[InvestmentRecord] = []

        self._mugamaru_account = Decimal("0")
        self._dse_interest_earned = Decimal("0")
        self._total_dividends_distributed = Decimal("0")

        self._processed_tx_ids: set[str] = set()

    @staticmethod
    def _to_decimal(value: Decimal | str | int) -> Decimal:
        return value if isinstance(value, Decimal) else Decimal(str(value))

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
        return [vars(record) for record in self._investment_history]

    def get_user_balance(self, user_id: str) -> Decimal:
        return self._fractional_ledger.get(user_id, Decimal("0"))

    def process_transaction(self,
                            user_id: str,
                            amount: Decimal | float | int,
                            tx_id: Optional[str] = None) -> Transaction:
        tx_id = tx_id or str(uuid.uuid4())
        amount_decimal = self._to_decimal(amount)

        if amount_decimal <= 0:
            logger.error("Invalid amount: %s", amount_decimal)
            return Transaction(
                tx_id=tx_id,
                user_id=user_id,
                amount=amount_decimal,
                status=TransactionStatus.FAILED,
                error_message="Invalid amount"
            )

        if tx_id in self._processed_tx_ids:
            logger.warning("Duplicate transaction detected: %s", tx_id)
            return Transaction(
                tx_id=tx_id,
                user_id=user_id,
                amount=amount_decimal,
                status=TransactionStatus.SWEPT,
                error_message="Duplicate transaction"
            )

        if amount_decimal < self.minimum_balance:
            logger.info(
                "Transaction %s for user %s is below the minimum sweep threshold %s and will be skipped",
                tx_id, user_id, self.minimum_balance
            )
            return Transaction(
                tx_id=tx_id,
                user_id=user_id,
                amount=amount_decimal,
                status=TransactionStatus.SKIPPED_INSUFFICIENT_BALANCE,
                error_message=f"Amount below minimum ({self.minimum_balance})"
            )

        sweep_amount = self._calculate_sweep(amount_decimal)
        if sweep_amount <= 0:
            logger.info("No sweep required for transaction %s", tx_id)
            self._processed_tx_ids.add(tx_id)
            return Transaction(
                tx_id=tx_id,
                user_id=user_id,
                amount=amount_decimal,
                status=TransactionStatus.SWEPT,
                sweep_amount=Decimal("0")
            )

        return self._execute_sweep(tx_id, user_id, amount_decimal, sweep_amount)

    def _calculate_sweep(self, amount: Decimal) -> Decimal:
        remainder = amount % self.SWEEP_ROUNDING
        if remainder == 0:
            return Decimal("0")
        return self.SWEEP_ROUNDING - remainder

    def _execute_sweep(self, tx_id: str, user_id: str, amount: Decimal, sweep_amount: Decimal) -> Transaction:
        mugamaru_share = (sweep_amount * self.MUGAMARU_PERCENT).quantize(Decimal("0.01"))
        vault_investment = sweep_amount - mugamaru_share

        self._mugamaru_account += mugamaru_share
        self._fractional_ledger[user_id] = self.get_user_balance(user_id) + vault_investment
        self._vault_balance += vault_investment

        logger.info(
            "Swept %s for user %s. Mugamaru: %s. Vault: %s.",
            sweep_amount, user_id, mugamaru_share, self._vault_balance
        )

        self._processed_tx_ids.add(tx_id)
        if self._vault_balance >= self.vault_threshold:
            self._trigger_dse_bond_purchase()

        return Transaction(
            tx_id=tx_id,
            user_id=user_id,
            amount=amount,
            status=TransactionStatus.SWEPT,
            sweep_amount=sweep_amount,
            fee=mugamaru_share
        )

    def _trigger_dse_bond_purchase(self) -> None:
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

            interest = (purchase_amount * (self.DSE_INTEREST_RATE / Decimal("4"))).quantize(Decimal("0.01"))
            self._dse_interest_earned += interest
            self._total_dividends_distributed += interest

            logger.info(
                "Purchased DSE bond %s for %s, earned interest %s.",
                investment_record.bond_id,
                purchase_amount,
                interest
            )
