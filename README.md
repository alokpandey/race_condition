# Race Condition Demo

A Python demonstration of race conditions and how to solve them using data partitioning.

## Description

This project demonstrates race conditions that occur when multiple workers try to acquire locks on the same Redis records, and shows how data partitioning can be used to avoid these issues.

The demo includes:
- Simulation of inventory records in Redis
- Demonstration of race conditions with multiple concurrent workers
- Implementation of data partitioning to eliminate contention
- Detailed comparison metrics between both approaches

## Requirements

- Redis server running on localhost:6379
- Python 3.x
- Python redis module (`pip install redis`)

## Usage

```bash
python3 race_condition.py
```

## How It Works

The demo runs two tests:

1. **Race Condition Test**: Multiple workers randomly select and try to update inventory records, causing contention and lock failures.

2. **Partitioned Approach**: Each worker is assigned a specific partition of inventory records, eliminating contention between workers.

The results show how partitioning improves success rates and performance by avoiding race conditions.

## Key Components

- `RaceConditionWorker`: Simulates workers that compete for the same records
- `PartitionedWorker`: Implements the solution using data partitioning
- Redis-based distributed locking mechanism
- Performance metrics collection and comparison

## Author

Augment AI

## License

This project is provided as an educational demonstration.
