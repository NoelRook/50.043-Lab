package simpledb.storage;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int pageNum;
    //something is here to store pages
    private ConcurrentHashMap<PageId, Page> pagesMap; // cache recently used pages into pagesMap
    
    // Add the lock manager
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.pageNum = numPages;
        this.pagesMap = new ConcurrentHashMap<>(numPages);
        this.lockManager = new LockManager(); // Initialize lock manager
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        
        // STEP 1: Acquire lock before accessing the page
        lockManager.acquireLock(tid, pid, perm);
        
        try {
            // STEP 2: Check if page is in buffer pool
            if (this.pagesMap.containsKey(pid)) { 
                // If there is a hit, return the page to the requester
                Page page = this.pagesMap.get(pid);
                // Update LRU order - remove and re-add to make it most recently used
                this.pagesMap.remove(pid);
                this.pagesMap.put(pid, page);
                return page;
            }
            
            // STEP 3: Page not in buffer pool, load from disk
            Page newPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);

            // STEP 4: Check if buffer pool is full, evict if necessary
            if (this.pagesMap.size() >= pageNum) { 
                this.evictPage(); 
            }

            // STEP 5: Add new page to buffer pool
            this.pagesMap.put(pid, newPage);
            return newPage;
            
        } catch (Exception e) {
            // If any error occurs, release the lock and re-throw
            lockManager.releaseLock(tid, pid, perm);
            if (e instanceof DbException) {
                throw (DbException) e;
            } else if (e instanceof TransactionAbortedException) {
                throw (TransactionAbortedException) e;
            } else {
                throw new DbException("Error retrieving page: " + e.getMessage());
            }
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // Release both read and write locks (if any)
        if (lockManager.holdsLock(tid, pid, Permissions.READ_WRITE)) {
            lockManager.releaseLock(tid, pid, Permissions.READ_WRITE);
        }
        if (lockManager.holdsLock(tid, pid, Permissions.READ_ONLY)) {
            lockManager.releaseLock(tid, pid, Permissions.READ_ONLY);
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        this.transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // If committing, flush all dirty pages for this transaction

        try {
            if(commit){
                flushPages(tid);
            }
            else{
                Iterator<PageId> iter = pagesMap.keySet().iterator();
                while (iter.hasNext()) {
                    PageId pid = iter.next();
                    Page page = pagesMap.get(pid);
                    if (page.isDirty() != null && page.isDirty().equals(tid)) {
                        Page tempPage = page.getBeforeImage();
                        pagesMap.remove(pid);
                        pagesMap.put(pid, tempPage);
                    }
                }
            }

            
        } catch (Exception e) {
            System.err.println("Error flushing pages for transaction " + tid + ": " + e.getMessage());

        }
        
        // Always release all locks held by the transaction
        lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        
        DbFile curTable = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pageArray = curTable.insertTuple(tid, t);
        
        for (Page pg : pageArray) {
            // Acquire write lock for the page before modifying
            lockManager.acquireLock(tid, pg.getId(), Permissions.READ_WRITE);
            
            pg.markDirty(true, tid);
            
            if (!this.pagesMap.containsKey(pg.getId()) && this.pagesMap.size() >= this.pageNum) {
                this.evictPage();
            }
            
            // Update LRU cache
            this.pagesMap.remove(pg.getId());
            this.pagesMap.put(pg.getId(), pg);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        
        // Get table id
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pageArray = dbFile.deleteTuple(tid, t);
        
        for (Page pg : pageArray) {
            // Acquire write lock for the page before modifying
            lockManager.acquireLock(tid, pg.getId(), Permissions.READ_WRITE);
            
            pg.markDirty(true, tid);
            
            if (!this.pagesMap.containsKey(pg.getId()) && this.pagesMap.size() >= this.pageNum) {
                this.evictPage();
            }
            
            // Update LRU cache
            this.pagesMap.remove(pg.getId());
            this.pagesMap.put(pg.getId(), pg);
        }   
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page p: this.pagesMap.values()) {
            this.flushPage(p.getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        this.pagesMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = this.pagesMap.get(pid);
        if (page != null && page.isDirty() != null) {
            // Write the page to disk
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (Page page : this.pagesMap.values()) {
            if (page.isDirty() != null && page.isDirty().equals(tid)) {
                this.flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // Find a page that is not dirty (to avoid writing dirty pages in NO STEAL mode)
        PageId pidToEvict = null;
        
        for (PageId pid : this.pagesMap.keySet()) {
            Page page = this.pagesMap.get(pid);
            if (page.isDirty() == null) { // Page is not dirty
                pidToEvict = pid;
                break;
            }
        }
        
        // If no clean page found, evict the first page (assuming STEAL policy)
        if (pidToEvict == null) {
            pidToEvict = this.pagesMap.keySet().iterator().next();
        }

        try {
            this.flushPage(pidToEvict);
        } catch (IOException e) {
            throw new DbException("Page could not be flushed.");
        }
        
        this.pagesMap.remove(pidToEvict);
    }
}