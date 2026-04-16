# Malicent

Malicent is a Python implementation of a transaction sweep engine that rounds transactions up to the next 100 units, invests the swept amount into a vault, and purchases DSE bonds when the vault reaches the configured threshold.

## Project structure

- `malicent/engine.py` — Core engine implementation.
- `malicent/cli.py` — Simple command-line interface.
- `malicent/__main__.py` — Enables `python -m malicent` execution.
- `tests/test_engine.py` — Unit tests for the engine behavior.
- `pyproject.toml` — Packaging configuration.

## Installation

Install the package into your environment:

```bash
python -m pip install -e .
```

## Usage

Process a transaction:

```bash
python -m malicent process user1 5150
```

Show engine status:

```bash
python -m malicent status
```

Show investment history:

```bash
python -m malicent history
```

Use the engine from Python:

```python
from decimal import Decimal
from malicent.engine import MalicentEngine

engine = MalicentEngine()
transaction = engine.process_transaction("user1", Decimal("5200"))
print(transaction)
print(engine.current_vault_balance)
```

## Testing

Run the test suite with:

```bash
python -m pytest
```

## Notes

- Transactions below the configured minimum balance will be skipped.
- Sweeps are rounded up to the next 100.
- 10% of each sweep is allocated to the Mugamaru fee pool.
- When the vault reaches the threshold, a DSE bond purchase is triggered.
