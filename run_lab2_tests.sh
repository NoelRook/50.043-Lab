PROJECT_DIR="$(pwd)"
cd "$PROJECT_DIR" || { echo "Project directory not found"; exit 1; }
echo "Running required unit tests for Lab 2..."

# Section 2.1: Filter and Join
ant runtest -Dtest=PredicateTest
ant runtest -Dtest=JoinPredicateTest
ant runtest -Dtest=FilterTest
ant runtest -Dtest=JoinTest

# Section 2.2: Aggregates
ant runtest -Dtest=IntegerAggregatorTest
ant runtest -Dtest=StringAggregatorTest
ant runtest -Dtest=AggregateTest

# Section 2.3: HeapFile Mutability
ant runtest -Dtest=HeapPageWriteTest
ant runtest -Dtest=HeapFileWriteTest
ant runtest -Dtest=BufferPoolWriteTest

# Section 2.4: Insertion and Deletion
ant runtest -Dtest=InsertTest

echo ""
echo "Running required system tests for Lab 2..."

# Section 2.1: Filter and Join
ant runsystest -Dtest=FilterTest
ant runsystest -Dtest=JoinTest

# Section 2.2: Aggregates
ant runsystest -Dtest=AggregateTest

# Section 2.4: Insertion and Deletion
ant runsystest -Dtest=InsertTest
ant runsystest -Dtest=DeleteTest

# Section 2.5: Page eviction
ant runsystest -Dtest=EvictionTest

echo ""
echo "All specified Lab 2 tests executed."