package simpledb.storage;

import java.io.*;
import java.util.*;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    // heapfile is a collection of pages

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.f.getAbsoluteFile().hashCode();        
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    // Calculate the offset in the file
    int pageSize = BufferPool.getPageSize();
    int pageNumber = pid.getPageNumber();
    int offset = pageSize * pageNumber;
    
    // Create a byte buffer to hold the page data
    byte[] pageData = new byte[pageSize];
    
    try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
        // Seek to the correct offset
        raf.seek(offset);
        // Read the page data
        raf.read(pageData);
        // Create and return the HeapPage
        return new HeapPage((HeapPageId) pid, pageData);
    } catch (IOException e) {
        throw new IllegalArgumentException("Could not read page with id " + pid);
    }
    }   

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    return new HeapFileIterator(this, tid);
}

private static class HeapFileIterator implements DbFileIterator {
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

}




