#!/bin/bash

PROJECT_DIR="$(pwd)"
cd "$PROJECT_DIR" || { echo "Project directory not found"; exit 1; }

echo "Running required unit tests for Lab 3..."

# Exercise 1: Lock acquisition and release in BufferPool
ant runtest -Dtest=LockingTest

# Exercise 2: End-to-end locking behavior
ant runtest -Dtest=TransactionTest

# Exercise 3: NO STEAL eviction logic
ant runtest -Dtest=AbortEvictionTest

# Exercise 4: Transaction lifecycle (commit/abort)
ant runtest -Dtest=TransactionTest

# Exercise 5: Deadlock detection
ant runtest -Dtest=DeadlockTest

echo ""
echo "Running required system tests for Lab 3..."

# Full system test for transactions and locking
ant runsystest -Dtest=TransactionTest
ant runsystest -Dtest=AbortEvictionTest
ant runsystest -Dtest=DeadlockTest

echo ""
echo "All specified Lab 3 tests executed."