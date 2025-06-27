package simpledb.storage;

import java.io.*;
import java.util.*;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * HeapFileIterator is the iterator for a HeapFile.
 * It reads tuples across pages for a given transaction.
 */


public class HeapFileIterator implements DbFileIterator {
    private final HeapFile heapFile;
    private final TransactionId tid;
    private Iterator<Tuple> tupleIterator;
    private int currentPageNo;
    
    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
    }
    
    @Override
    public void open() throws DbException, TransactionAbortedException {
        currentPageNo = 0;
        tupleIterator = getPageTuples(currentPageNo);
    }
    
    private Iterator<Tuple> getPageTuples(int pageNo) throws DbException, TransactionAbortedException {
        if (pageNo >= 0 && pageNo < heapFile.numPages()) {
            HeapPageId pid = new HeapPageId(heapFile.getId(), pageNo);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }
        return null;
    }
    
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (tupleIterator == null) {
            return false;
        }
        
        while (!tupleIterator.hasNext()) {
            currentPageNo++;
            if (currentPageNo >= heapFile.numPages()) {
                return false;
            }
            tupleIterator = getPageTuples(currentPageNo);
        }
        return true;
    }
    
    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return tupleIterator.next();
    }
    
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }
    
    @Override
    public void close() {
        tupleIterator = null;
    }
}







