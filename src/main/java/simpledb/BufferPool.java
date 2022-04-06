package simpledb;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 16;


    private final Map<PageId, Page> pages;

    private final LockManager lockManager = new LockManager();

    private final int numPages;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        pages = new LinkedHashMap<>(16, 0.75f, true);
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
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        if (Objects.equals(perm, Permissions.READ_ONLY)) {
            try {
                lockManager.acquireReadLock(pid, tid, TimeUnit.SECONDS.toNanos(10));
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new TransactionAbortedException();
            }
        }
        if (Objects.equals(perm, Permissions.READ_WRITE)) {
            try {
                lockManager.acquireWriteLock(pid, tid, TimeUnit.SECONDS.toNanos(10));
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new TransactionAbortedException();
            }
        }
        // some code goes here
        synchronized (this) {
            Page page = pages.get(pid);
            if (page == null) {
                if (this.pages.size() >= numPages) {
                    evictPage();
                }
                DbFile databaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                page = databaseFile.readPage(pid);
                this.pages.put(pid, page);
            }
            return page;
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
    public void releasePage(TransactionId tid, PageId pid) {
        lockManager.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        if (commit) {
            flushPages(tid);
        } else {
            restorePages(tid);
        }
        lockManager.release(tid);
    }


    private synchronized void restorePages(TransactionId tid) {
        Set<PageId> pageIds = lockManager.getPageId(tid);
        for (PageId pageId : pageIds) {
            pages.remove(pageId);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, TransactionAbortedException {
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
        try {
            for (Page page : databaseFile.insertTuple(tid, t)) {
                synchronized (this) {
                    this.pages.put(page.getId(), page);
                }
                page.markDirty(true, tid);
            }
        } catch (IOException e) {
            throw new DbException("");
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        try {
            for (Page page : databaseFile.deleteTuple(tid, t)) {
                synchronized (this) {
                    this.pages.put(page.getId(), page);
                }
                page.markDirty(true, tid);
            }
        } catch (IOException e) {
            throw new DbException("");
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        Set<PageId> set = new HashSet<>(pages.keySet());
        for (PageId page : set) {
            flushPage(page);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = pages.get(pid);
        if (page != null) {
            TransactionId dirty = page.isDirty();
            if (dirty != null) {

                Database.getLogFile().logWrite(dirty, page.getBeforeImage(), page);
                Database.getLogFile().force();

                DbFile databaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                databaseFile.writePage(page);
                page.markDirty(false, null);

                page.setBeforeImage();
            }
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        if (tid == null) {
            return;
        }
        List<Page> values = new ArrayList<>(pages.values());
        for (Page page : values) {
            if (tid.equals(page.isDirty())) {
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        Iterator<Map.Entry<PageId, Page>> iterator = pages.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<PageId, Page> next = iterator.next();
            Page page = next.getValue();
            if (page.isDirty() == null) {
                iterator.remove();
                return;
            }
        }
        throw new DbException("");
    }
}
