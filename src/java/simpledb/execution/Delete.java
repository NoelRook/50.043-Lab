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

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /*
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */


    private final TransactionId transactionId;
    private OpIterator child;
    private boolean hasDeleted;
    private TupleDesc tupleDesc;

    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.transactionId = t;
        this.child = child;
        this.hasDeleted = false;

        //create output tuple descriptor (single INT field)
        Type[] typeAr = new Type[]{Type.INT_TYPE};
        String[] fieldAr = new String[]{"deleted_count"};
        this.tupleDesc = new TupleDesc(typeAr, fieldAr);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
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
        hasDeleted = false;
    }

    /*
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (hasDeleted){
            return null; //only return one tuple with the count
        }

        int count = 0;
        while (child.hasNext()){
            Tuple t= child.next();
            try{
                Database.getBufferPool().deleteTuple(transactionId, t);
                count++;
            } catch(Exception e){
                throw new DbException("Failed to delete tuple "+ e.toString());
            }
        }

        //create and return result tuple
        Tuple result = new Tuple(tupleDesc);
        result.setField(0, new IntField(count));
        hasDeleted = true;
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
        if (children.length!=1){
            throw new IllegalArgumentException("Expect 1 child operator");
        }
        this.child = children[0];
    }

}
