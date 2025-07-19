
PROJECT_DIR="$(pwd)"
cd "$PROJECT_DIR" || { echo "Project directory not found"; exit 1; }
echo "Running required unit tests for Lab 1..."

# Section 2.2: Tuples and TupleDesc
ant runtest -Dtest=TupleTest
ant runtest -Dtest=TupleDescTest

# Section 2.3: Catalog
ant runtest -Dtest=CatalogTest

# Section 2.5: HeapPage structure
ant runtest -Dtest=HeapPageIdTest
ant runtest -Dtest=RecordIdTest
ant runtest -Dtest=HeapPageReadTest

# Section 2.5: HeapFile access
ant runtest -Dtest=HeapFileReadTest

echo ""
echo "Running required system tests for Lab 1..."

# Section 2.6: SeqScan operator
ant runsystest -Dtest=ScanTest

echo ""
echo "All specified Lab 1 tests executed."
