import argparse
from decimal import Decimal
from malicent.engine import MalicentEngine, TransactionStatus


def _print_transaction(transaction):
    print("Transaction")
    print("-----------")
    print(f"ID: {transaction.tx_id}")
    print(f"User: {transaction.user_id}")
    print(f"Amount: {transaction.amount}")
    print(f"Status: {transaction.status.value}")
    print(f"Sweep amount: {transaction.sweep_amount}")
    print(f"Fee: {transaction.fee}")
    if transaction.error_message:
        print(f"Error: {transaction.error_message}")


def _print_summary(engine: MalicentEngine):
    print("Malicent Engine Summary")
    print("----------------------")
    print(f"Vault balance: {engine.current_vault_balance}")
    print(f"Mugamaru account: {engine.mugamaru_account}")
    print(f"DSE interest earned: {engine.dse_interest_earned}")
    print(f"Dividends distributed: {engine.total_dividends_distributed}")
    print(f"Vault threshold: {engine.vault_threshold}")
    print(f"Minimum balance: {engine.minimum_balance}")
    print(f"Users tracked: {len(engine.fractional_ledger)}")


def main():
    parser = argparse.ArgumentParser(prog="malicent", description="Malicent transaction sweep engine CLI")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    subparsers = parser.add_subparsers(dest="command", required=True)

    process_parser = subparsers.add_parser("process", help="Process a transaction")
    process_parser.add_argument("user_id", help="User identifier")
    process_parser.add_argument("amount", type=Decimal, help="Transaction amount")
    process_parser.add_argument("--tx-id", help="Optional transaction ID")

    balance_parser = subparsers.add_parser("balance", help="Show a user balance")
    balance_parser.add_argument("user_id", help="User identifier")

    status_parser = subparsers.add_parser("status", help="Show engine status")

    history_parser = subparsers.add_parser("history", help="Show investment history")

    args = parser.parse_args()
    engine = MalicentEngine()

    if args.command == "process":
        transaction = engine.process_transaction(args.user_id, args.amount, args.tx_id)
        _print_transaction(transaction)
        return 0

    if args.command == "balance":
        print(f"Balance for {args.user_id}: {engine.get_user_balance(args.user_id)}")
        return 0

    if args.command == "status":
        _print_summary(engine)
        return 0

    if args.command == "history":
        for record in engine.investment_history:
            print(record)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
