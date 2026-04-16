from decimal import Decimal
import pytest

from malicent.engine import MalicentEngine, TransactionStatus


def test_invalid_transaction_amount():
    engine = MalicentEngine()
    transaction = engine.process_transaction("user1", Decimal("0"))
    assert transaction.status == TransactionStatus.FAILED
    assert "Invalid amount" in transaction.error_message


def test_duplicate_transaction_is_detected():
    engine = MalicentEngine()
    first = engine.process_transaction("user1", Decimal("5500"), tx_id="tx-123")
    second = engine.process_transaction("user1", Decimal("5500"), tx_id="tx-123")

    assert first.status == TransactionStatus.SWEPT
    assert second.status == TransactionStatus.SWEPT
    assert second.error_message == "Duplicate transaction"


def test_threshold_skips_small_transaction():
    engine = MalicentEngine(minimum_balance=Decimal("5000"))
    transaction = engine.process_transaction("user1", Decimal("4500"))

    assert transaction.status == TransactionStatus.SKIPPED_INSUFFICIENT_BALANCE
    assert "Amount below minimum" in transaction.error_message
    assert engine.current_vault_balance == Decimal("0")


def test_sweep_rounds_up_to_next_100():
    engine = MalicentEngine(minimum_balance=Decimal("1000"))
    transaction = engine.process_transaction("user1", Decimal("5150"))

    assert transaction.status == TransactionStatus.SWEPT
    assert transaction.sweep_amount == Decimal("50")
    assert engine.current_vault_balance == Decimal("45.00")
    assert engine.mugamaru_account == Decimal("5.00")
    assert engine.get_user_balance("user1") == Decimal("45.00")


def test_vault_threshold_triggers_bond_purchase():
    engine = MalicentEngine(vault_threshold=Decimal("40"), minimum_balance=Decimal("0"))
    engine.process_transaction("user1", Decimal("150"), tx_id="tx-100")

    assert engine.current_vault_balance == Decimal("5.00")
    assert len(engine.investment_history) == 1
    assert engine.dse_interest_earned > Decimal("0")


def test_investment_history_records_bond_purchases():
    engine = MalicentEngine(vault_threshold=Decimal("40"), minimum_balance=Decimal("0"))
    engine.process_transaction("user1", Decimal("150"), tx_id="tx-101")
    engine.process_transaction("user1", Decimal("250"), tx_id="tx-102")

    assert len(engine.investment_history) == 2
    assert all(record["status"] == "SETTLED_AT_DSE" for record in engine.investment_history)
