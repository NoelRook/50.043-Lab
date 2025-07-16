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
 * w
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
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.seek(page.getId().getPageNumber() * Database.getBufferPool().getPageSize());
            raf.write(page.getPageData());
            raf.close();
        }
        
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
        
        List<Page> res = new ArrayList<>();
        for (int i = 0; i < this.numPages(); i++) {
            HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(
                    tid,
                    new HeapPageId(this.getId(), i),
                    Permissions.READ_WRITE
            );
            if (curPage.getNumEmptySlots() > 0) {
                curPage.insertTuple(t);
                res.add(curPage);
                return res;
            } else {
                Database.getBufferPool().unsafeReleasePage(tid, curPage.pid);
            }
        }
        HeapPage curPage = new HeapPage(
                new HeapPageId(this.getId(), this.numPages()),
                HeapPage.createEmptyPageData()
        );
        curPage.insertTuple(t);
        writePage(curPage);
        res.add(curPage);
        return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here

        PageId pageId = t.getRecordId().getPageId();
        HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        curPage.deleteTuple(t);

        ArrayList<Page> res = new ArrayList<>();
        res.add(curPage);
        return res;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    return new HeapFileIterator(this, tid);
}

}