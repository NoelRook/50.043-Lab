package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /*
      Constructor.
     
      @param t
                 The transaction running the insert.
      @param child
                 The child operator from which to read tuples to be inserted.
      @param tableId
                 The table in which to insert tuples.
      @throws DbException
                  if TupleDesc of child differs from table into which we are to
                  insert.
     */

     private final TransactionId transactionId;
     private OpIterator child;
     private final int tableId;
     private boolean hasInserted;
     private TupleDesc tupleDesc;


    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.transactionId = t;
        this.child = child;
        this.tableId = tableId;
        this.hasInserted = false;

        //verify that tuple descriptor matches the table
        TupleDesc childTd = child.getTupleDesc();
        TupleDesc tableTd = Database.getCatalog().getTupleDesc(tableId);

        if (!childTd.equals(tableTd)){
            throw new DbException("TupleDesc mismatch: child and table differ");
        }

        Type[] typeAr = new Type[]{Type.INT_TYPE};
        String[] fieldAr = new String[]{"inserted_count"};
        this.tupleDesc = new TupleDesc(typeAr, fieldAr);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        hasInserted = false;
    }

    /*
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (hasInserted){
            return null;
        }

        int count = 0;
        while (child.hasNext()){
            Tuple t= child.next();
            try{
                Database.getBufferPool().insertTuple(transactionId, tableId, t);
                count++;
            } catch (Exception e){
                throw new DbException("Failed to insert tuple "+ e.toString());
            }
        }
        //create and return the result tuple
        Tuple result = new Tuple(tupleDesc);
        result.setField(0, new IntField(count));
        hasInserted = true;
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children.length != 1){
            throw new IllegalArgumentException("Expect 1 child operator");
        }
        this.child = children[0];
    }
}
