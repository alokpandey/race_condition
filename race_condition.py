#!/usr/bin/env python3
"""
=============================================================================
Race Condition Demo - Proof of Concept
=============================================================================

Author: Augment AI
Date: June 2024

Description:
    This script demonstrates race conditions that occur when multiple workers
    try to acquire locks on the same Redis records, and shows how data
    partitioning can be used to avoid these issues.

Features:
    - Simulates inventory records in Redis
    - Demonstrates race conditions with multiple concurrent workers
    - Shows how partitioning eliminates contention and improves performance
    - Provides detailed comparison metrics between both approaches

Usage:
    python3 race_condition_demo.py

Requirements:
    - Redis server running on localhost:6379
    - Python redis module (pip install redis)
=============================================================================
"""

# Import necessary modules for the demo
import random          # For generating random numbers
import threading       # For creating and managing threads
import time            # For timing operations and simulating processing delays
import uuid            # For generating unique identifiers for locks
from concurrent.futures import ThreadPoolExecutor  # For managing thread pools

# Try to import Redis, with a helpful error message if it fails
try:
    import redis       # Redis client library for Python
except ImportError:
    # Display a helpful error message if Redis module is not installed
    print("Error: Redis module not found. Please install it with:")
    print("pip install redis")
    exit(1)  # Exit the program if Redis module is not available

# ===== Inventory Generator =====

def generate_inventory_data(redis_client, num_records=1000):
    """Generate and load inventory records into Redis."""
    # Inform the user about the operation being performed
    print(f"Generating {num_records} inventory records...")

    # Clear all existing data in the Redis database to start fresh
    redis_client.flushdb()

    # Loop through and create the specified number of inventory records
    for i in range(num_records):
        # Create a unique key for each inventory record
        inventory_id = f"inventory:{i}"

        # Define the data structure for each inventory record
        inventory_data = {
            "id": i,                              # Unique identifier
            "quantity": random.randint(1, 100),  # Random quantity between 1 and 100
            "trace": "initial",                 # Initial trace value (will be updated by workers)
            "last_updated": time.time()         # Timestamp of when the record was created
        }

        # Store each field of the inventory record in Redis using hset
        # (Using individual hset calls instead of deprecated hmset)
        for key, value in inventory_data.items():
            redis_client.hset(inventory_id, key, value)

        # Print progress periodically for large datasets
        if i % 1000 == 0 and i > 0:
            print(f"Generated {i} records...")

    # Inform the user that the operation is complete
    print(f"Successfully generated {num_records} inventory records in Redis.")

# ===== Race Condition Worker =====
# This class simulates a worker that tries to update random inventory records
# Multiple workers will compete for the same records, causing race conditions

class RaceConditionWorker:
    def __init__(self, redis_client, worker_id, num_records, lock_timeout=1):
        # Store the Redis client connection for database operations
        self.redis = redis_client
        # Unique identifier for this worker
        self.worker_id = worker_id
        # Total number of inventory records in the system
        self.num_records = num_records
        # How long a lock should be held (in seconds)
        self.lock_timeout = lock_timeout

        # Statistics to track worker performance
        self.successful_updates = 0  # Counter for successful updates
        self.failed_updates = 0      # Counter for failed lock acquisitions
        self.timeouts = 0            # Counter for simulated timeouts

    def update_random_inventory(self, num_attempts):
        """Try to update random inventory records."""
        # Attempt to update inventory records the specified number of times
        for _ in range(num_attempts):
            # Select a random inventory record - THIS IS KEY TO DEMONSTRATING RACE CONDITIONS
            # Since each worker selects random records, multiple workers may try to update the same record
            inventory_id = f"inventory:{random.randint(0, self.num_records - 1)}"
            # Create a lock name based on the inventory ID
            lock_name = f"lock:{inventory_id}"

            # Generate a unique lock value (UUID) to ensure only the owner can release the lock
            lock_value = str(uuid.uuid4())

            try:
                # Simulate a timeout occasionally (5% chance)
                # This simulates network issues or other problems that might cause timeouts
                if random.random() < 0.05:
                    self.timeouts += 1  # Increment timeout counter
                    continue  # Skip to the next attempt

                # Try to acquire the lock with timeout
                # The lock will automatically expire after timeout_seconds
                timeout_seconds = max(1, int(self.lock_timeout))  # Ensure minimum 1 second timeout
                # Use Redis SET with NX (Only set if key doesn't exist) and EX (expiration time)
                # This is the Redis distributed locking pattern
                acquired = self.redis.set(
                    lock_name,      # The lock key
                    lock_value,     # The unique value to identify the lock owner
                    ex=timeout_seconds,  # Expiration time in seconds
                    nx=True         # Only set if the key doesn't already exist
                )

                if acquired:  # If we successfully acquired the lock
                    try:
                        # Simulate some processing time (database operations, calculations, etc.)
                        time.sleep(0.1)

                        # Update the inventory record with this worker's ID
                        self.redis.hset(
                            inventory_id,   # The inventory record key
                            "trace",        # The field to update
                            f"updated_by_worker_{self.worker_id}"  # New value with worker ID
                        )
                        # Update the last_updated timestamp
                        self.redis.hset(
                            inventory_id,
                            "last_updated",
                            time.time()  # Current timestamp
                        )
                        # Increment the success counter
                        self.successful_updates += 1
                    finally:
                        # IMPORTANT: Always release the lock, even if an error occurred
                        # Use a Lua script to ensure atomic release of the lock
                        # This script only deletes the lock if it still belongs to this worker
                        release_script = """
                        if redis.call("get", KEYS[1]) == ARGV[1] then
                            return redis.call("del", KEYS[1])
                        else
                            return 0
                        end
                        """
                        self.redis.eval(release_script, 1, lock_name, lock_value)
                else:
                    # Failed to acquire lock - another worker already has it
                    # This is where race conditions manifest
                    self.failed_updates += 1
            except Exception as e:
                # Handle any unexpected errors
                print(f"Error in worker {self.worker_id}: {e}")
                self.failed_updates += 1

# ===== Partitioned Worker =====
# This class implements the solution to race conditions by partitioning the data
# Each worker is assigned a specific partition of inventory records
# This eliminates contention between workers since they never compete for the same records

class PartitionedWorker:
    def __init__(self, redis_client, worker_id, num_records, num_workers, lock_timeout=1):
        # Store the Redis client connection for database operations
        self.redis = redis_client
        # Unique identifier for this worker
        self.worker_id = worker_id
        # Total number of inventory records in the system
        self.num_records = num_records
        # Total number of workers in the system
        self.num_workers = num_workers
        # How long a lock should be held (in seconds)
        self.lock_timeout = lock_timeout

        # Calculate partition boundaries - THIS IS THE KEY TO AVOIDING RACE CONDITIONS
        # Each worker is assigned a specific range of inventory records
        self.partition_size = num_records // num_workers  # Size of each partition
        self.start_id = worker_id * self.partition_size   # First record ID in this worker's partition
        # Last record ID in this worker's partition (handle the last worker specially)
        self.end_id = (worker_id + 1) * self.partition_size if worker_id < num_workers - 1 else num_records

        # Statistics to track worker performance
        self.successful_updates = 0  # Counter for successful updates
        self.failed_updates = 0      # Counter for failed lock acquisitions
        self.timeouts = 0            # Counter for timeouts (should be 0 with partitioning)

    def update_partition(self):
        """Update inventory records in this worker's partition."""
        # Log which partition this worker is processing
        print(f"Worker {self.worker_id} processing partition: {self.start_id} to {self.end_id-1}")

        # Process each inventory record in this worker's partition
        # Unlike the race condition approach, this worker only processes its assigned records
        for inventory_index in range(self.start_id, self.end_id):
            # Get the inventory ID for this index
            inventory_id = f"inventory:{inventory_index}"
            # Create a lock name based on the inventory ID
            lock_name = f"lock:{inventory_id}"

            # Generate a unique lock value (UUID) to ensure only the owner can release the lock
            lock_value = str(uuid.uuid4())

            try:
                # Try to acquire the lock with timeout
                # Even though we're partitioning, we still use locks as a best practice
                # However, lock acquisition should always succeed since no other worker will try to lock this record
                timeout_seconds = max(1, int(self.lock_timeout))  # Ensure minimum 1 second timeout
                acquired = self.redis.set(
                    lock_name,      # The lock key
                    lock_value,     # The unique value to identify the lock owner
                    ex=timeout_seconds,  # Expiration time in seconds
                    nx=True         # Only set if the key doesn't already exist
                )

                if acquired:  # If we successfully acquired the lock (should always succeed with partitioning)
                    try:
                        # Simulate some processing time (database operations, calculations, etc.)
                        time.sleep(0.1)

                        # Update the inventory record with this worker's ID
                        self.redis.hset(
                            inventory_id,   # The inventory record key
                            "trace",        # The field to update
                            f"updated_by_worker_{self.worker_id}"  # New value with worker ID
                        )
                        # Update the last_updated timestamp
                        self.redis.hset(
                            inventory_id,
                            "last_updated",
                            time.time()  # Current timestamp
                        )
                        # Increment the success counter
                        self.successful_updates += 1
                    finally:
                        # IMPORTANT: Always release the lock, even if an error occurred
                        # Use a Lua script to ensure atomic release of the lock
                        # This script only deletes the lock if it still belongs to this worker
                        release_script = """
                        if redis.call("get", KEYS[1]) == ARGV[1] then
                            return redis.call("del", KEYS[1])
                        else
                            return 0
                        end
                        """
                        self.redis.eval(release_script, 1, lock_name, lock_value)
                else:
                    # Failed to acquire lock - this should never happen with proper partitioning
                    # If it does happen, it indicates a problem with the partitioning logic
                    self.failed_updates += 1
                    print(f"WARNING: Worker {self.worker_id} failed to acquire lock for {inventory_id} in its own partition!")
            except Exception as e:
                # Handle any unexpected errors
                print(f"Error in worker {self.worker_id}: {e}")
                self.failed_updates += 1

# ===== Test Functions =====
# These functions run the tests for both approaches and collect statistics

def run_race_condition_test(redis_client, num_workers, num_records, attempts_per_worker):
    """Run the race condition test with multiple workers accessing random records."""
    # Print a header to indicate which test is running
    print("\n" + "="*50)
    print("Running with potential race conditions...")
    print("="*50)

    # Record the start time to measure performance
    start_time = time.time()

    # Create lists to store worker objects and thread objects
    workers = []  # List of worker objects
    threads = []  # List of thread objects

    # Create and start a thread for each worker
    for i in range(num_workers):
        # Create a worker with a unique ID
        worker = RaceConditionWorker(redis_client, i, num_records)
        workers.append(worker)

        # Create a thread that will run the worker's update_random_inventory method
        # Each worker will attempt to update 'attempts_per_worker' random records
        thread = threading.Thread(target=worker.update_random_inventory, args=(attempts_per_worker,))
        threads.append(thread)

        # Start the thread (worker begins processing)
        thread.start()

    # Wait for all threads to complete before continuing
    for thread in threads:
        thread.join()

    # Record the end time to calculate total execution time
    end_time = time.time()

    # Aggregate statistics from all workers
    total_successful = sum(w.successful_updates for w in workers)  # Total successful updates
    total_failed = sum(w.failed_updates for w in workers)          # Total failed lock acquisitions
    total_timeouts = sum(w.timeouts for w in workers)              # Total timeouts
    total_time = end_time - start_time                            # Total execution time

    # Print the results
    print("\nRace Condition Results:")
    print(f"Total successful updates: {total_successful}")
    print(f"Total failed updates: {total_failed}")
    print(f"Total timeouts: {total_timeouts}")
    print(f"Total execution time: {total_time:.2f} seconds")

    # Calculate and print the success rate
    success_rate = total_successful / (num_workers * attempts_per_worker) * 100
    print(f"Success rate: {success_rate:.2f}%")

    # Return a dictionary with all the statistics
    return {
        "total_successful": total_successful,
        "total_failed": total_failed,
        "total_timeouts": total_timeouts,
        "total_time": total_time,
        "success_rate": success_rate
    }

def run_partitioned_test(redis_client, num_workers, num_records):
    """Run the partitioned test where each worker processes its own partition of records."""
    # Print a header to indicate which test is running
    print("\n" + "="*50)
    print("Running with partitioned approach...")
    print("="*50)

    # Record the start time to measure performance
    start_time = time.time()

    # Create lists to store worker objects and thread objects
    workers = []  # List of worker objects
    threads = []  # List of thread objects

    # Create and start a thread for each worker
    for i in range(num_workers):
        # Create a partitioned worker with a unique ID
        # Note: We pass num_workers so each worker can calculate its partition boundaries
        worker = PartitionedWorker(redis_client, i, num_records, num_workers)
        workers.append(worker)

        # Create a thread that will run the worker's update_partition method
        # Unlike the race condition test, each worker only processes its assigned partition
        thread = threading.Thread(target=worker.update_partition)
        threads.append(thread)

        # Start the thread (worker begins processing)
        thread.start()

    # Wait for all threads to complete before continuing
    for thread in threads:
        thread.join()

    # Record the end time to calculate total execution time
    end_time = time.time()

    # Aggregate statistics from all workers
    total_successful = sum(w.successful_updates for w in workers)  # Total successful updates
    total_failed = sum(w.failed_updates for w in workers)          # Total failed lock acquisitions
    total_timeouts = sum(w.timeouts for w in workers)              # Total timeouts (should be 0)
    total_time = end_time - start_time                            # Total execution time

    # Print the results
    print("\nPartitioned Approach Results:")
    print(f"Total successful updates: {total_successful}")
    print(f"Total failed updates: {total_failed}")
    print(f"Total timeouts: {total_timeouts}")
    print(f"Total execution time: {total_time:.2f} seconds")

    # Calculate and print the success rate
    # Note: With partitioning, we expect to process exactly num_records records
    # (one update per record, with each worker handling its partition)
    success_rate = total_successful / num_records * 100
    print(f"Success rate: {success_rate:.2f}%")

    # Return a dictionary with all the statistics
    return {
        "total_successful": total_successful,
        "total_failed": total_failed,
        "total_timeouts": total_timeouts,
        "total_time": total_time,
        "success_rate": success_rate
    }

def main():
    """Main function that runs both tests and compares the results."""
    # Configuration parameters for the tests
    num_records = 1000        # Total number of inventory records to create
    num_workers = 20          # Number of concurrent worker threads to run
    # Calculate how many records each worker should attempt to update
    # This ensures a fair comparison between the two approaches
    attempts_per_worker = num_records // num_workers  # Each worker attempts to update its share of records

    # Connect to Redis server
    try:
        # Create a Redis client with decode_responses=True to get string values instead of bytes
        redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
        # Test the connection with a ping
        redis_client.ping()
        print("Connected to Redis")
    except redis.ConnectionError:
        # Handle connection errors gracefully
        print("Failed to connect to Redis. Make sure Redis server is running.")
        return  # Exit the function if Redis is not available

    # Step 1: Generate initial inventory data for the first test
    generate_inventory_data(redis_client, num_records)

    # Step 2: Run the race condition test (random access pattern)
    race_results = run_race_condition_test(redis_client, num_workers, num_records, attempts_per_worker)

    # Step 3: Regenerate inventory data for a fair comparison
    # This ensures both tests start with the same data state
    generate_inventory_data(redis_client, num_records)

    # Step 4: Run the partitioned test (each worker has its own partition)
    partitioned_results = run_partitioned_test(redis_client, num_workers, num_records)

    # Step 5: Print comparison results
    print("\n" + "="*50)
    print("COMPARISON RESULTS")
    print("="*50)

    # Side-by-side comparison of key metrics
    print("\nRace Condition vs Partitioned Approach:")
    print(f"Success Rate: {race_results['success_rate']:.2f}% vs {partitioned_results['success_rate']:.2f}%")
    print(f"Execution Time: {race_results['total_time']:.2f}s vs {partitioned_results['total_time']:.2f}s")

    # Calculate improvement metrics
    # How much better is the partitioned approach in terms of success rate?
    success_improvement = partitioned_results['success_rate'] - race_results['success_rate']
    # How much faster is the partitioned approach (as a percentage)?
    time_improvement = (race_results['total_time'] - partitioned_results['total_time']) / race_results['total_time'] * 100

    # Print the improvement metrics
    print("\nImprovement with Partitioning:")
    print(f"Success Rate Improvement: {success_improvement:.2f}%")
    print(f"Execution Time Improvement: {time_improvement:.2f}%")

# Standard Python idiom to only run the main function if this script is executed directly
# (not imported as a module)
if __name__ == "__main__":
    main()  # Run the main function


