package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    // Fields
    private final LockManager lockManager;
    private final int maxNumPages;
    // Use a LinkedHashMap to maintain order of insertions
    // if doing LFU policy we can remove the head of the list
    private final Map<PageId, Page> pageMap;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.lockManager = new LockManager();
        this.maxNumPages = numPages;
        this.pageMap = new ConcurrentHashMap<>();
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
        // some code goes here
        try {
            this.lockManager.acquireLock(tid, pid, perm);
        } catch (InterruptedException e) {
        }
        if (this.pageMap.containsKey(pid)) {
            Page page = this.pageMap.get(pid);
            this.pageMap.put(pid, page);
            return page;
        }
        // check if we need to evict page
        if (checkBufferPoolSizeFull())
            evictPage();
        // page is absent, retrieve from disk
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = dbFile.readPage(pid);
        this.pageMap.put(pid, page);
        return page;
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        this.lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        return this.lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        if (commit) {
            logPages(tid);
        } else { // ABORT
            resetPages(tid);
        }
        this.lockManager.releaseLocks(tid);
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
        // some code goes here
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = file.insertTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            this.pageMap.put(page.getId(), page);
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
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        PageId pageId = t.getRecordId().getPageId();
        DbFile file = Database.getCatalog().getDatabaseFile(pageId.getTableId());
        List<Page> dirtyPages = file.deleteTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            this.pageMap.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        for (PageId pageId : this.pageMap.keySet())
            flushPage(pageId);
    }

    private synchronized void discardPages(TransactionId tid) {
        Set<PageId> pageList = this.lockManager.getTransactionPageList(tid);
        for (PageId pageId : pageList) {
            discardPage(pageId);
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
        // some code goes here
        if (!this.pageMap.containsKey(pid)) return;
        this.pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        Page page = this.pageMap.get(pid);
        TransactionId lastTouchedId = page.isDirty();
        // check to see if page is dirty, only write pages that are
        if (lastTouchedId != null) {
            Database.getLogFile().logWrite(lastTouchedId, page.getBeforeImage(), page);
            Database.getLogFile().force();
            HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
            file.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // use LockManager to get the list of pages that this transaction has locks to.
        Set<PageId> pageList = this.lockManager.getTransactionPageList(tid);
        if (pageList == null) return;
        for (PageId pageId : pageList) {
            if (this.pageMap.containsKey(pageId))
                flushPage(pageId);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        for (Map.Entry<PageId, Page> entry : this.pageMap.entrySet()) {
            this.pageMap.remove(entry.getKey());
            return;
        }

        // find the first non-dirty page.
//        for (Map.Entry<PageId, Page> entry : this.pageMap.entrySet()) {
//            if (entry.getValue().isDirty() == null) {
//                this.pageMap.remove(entry.getKey());
//                return;
//            }
//        }
//         throw new DbException("Cannot evict dirty page!");
    }

    ///////////////////////////////////////////////////////////////
    //                     Helper Methods                        //
    ///////////////////////////////////////////////////////////////

    private boolean checkBufferPoolSizeFull() {
        return this.pageMap.size() >= this.maxNumPages;
    }

    private synchronized void resetPages(TransactionId tid) {
        Set<PageId> pageList = this.lockManager.getTransactionPageList(tid);
        for (PageId pid : pageList) {
            HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);
            page.markDirty(false, tid);
            this.pageMap.put(pid, page);
        }
    }

    /**
     * Logs all changes a Transaction upon a Commit statement. This allows SimpleDB to have a NO_FORCE policy.
     * @param tid TransactionId for transaction doing a commit.
     * @throws IOException
     */
    private synchronized void logPages(TransactionId tid) throws IOException {
        Set<PageId> pageList = this.lockManager.getTransactionPageList(tid);
        for (PageId pid : pageList) {
            if (this.pageMap.containsKey(pid)) {
                Page page = this.pageMap.get(pid);
                TransactionId dirtier = page.isDirty();

                if (dirtier != null) {
                    Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
                    Database.getLogFile().force();
                }
                page.setBeforeImage();
            }
        }
    }
}
