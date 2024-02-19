package simpledb;

import java.util.*;

/**
 * Lock Manager for transactions.
 * Will manage the acquiring and releasing of lock on behalf of transactions.
 */
public class LockManager {

    private enum LockType {
        SHARED,
        EXCLUSIVE,
        UNUSED
    }

    private class Lock {
        private LockType type;
        private Set<TransactionId> sharedLock;

        public Lock(LockType type) {
            this.type = type;
            this.sharedLock = new HashSet<>();
        }

        public boolean isSharedLock() {
            return this.type == LockType.SHARED;
        }

        public void setLockType(LockType type) {
            this.type = type;
        }

        public void addTransactionId(TransactionId tid) {
            this.sharedLock.add(tid);
        }

        public void removeTransactionId(TransactionId tid) {
            this.sharedLock.remove(tid);
        }

        public Set<TransactionId> getSharedLock() {
            return this.sharedLock;
        }
    }

    // Fields
    // implement a page granularity
    // have each page have its own lock
    private Map<PageId, Lock> pageLocks;
    // have a list of all the pages a specific transaction holds locks to
    private Map<TransactionId, Set<PageId>> transactionIdListMap;

    /**
     * Constructs a LockManager with empty fields.
     */
    public LockManager() {
        this.pageLocks = new HashMap<>();
        this.transactionIdListMap = new HashMap<>();
    }

    /**
     * Transaction will try to acquire a lock on a page.
     * Will gain access if page is READ_ONLY.
     * Will wait if another transaction holds an exclusive lock to the page.
     * Will wait if trying to do a write to a page while other transactions hold lock to the same page.
     * Will upgrade lock status to READ_WRITE when trying to do a write while being the only transaction holding a
     * lock to the page.
     * @param tid TransactionId of transaction trying to acquire the lock.
     * @param pageId PageId of the page the transaction is trying to acquire the lock for.
     * @param perm Permission the transaction is asking for (Read/Write)
     */
    public synchronized void acquireLock(TransactionId tid, PageId pageId, Permissions perm) throws InterruptedException {
        this.pageLocks.putIfAbsent(pageId, new Lock(LockType.UNUSED));
        this.transactionIdListMap.putIfAbsent(tid, new HashSet<>());

        Lock lock = this.pageLocks.get(pageId);
        Set<PageId> transactionList = this.transactionIdListMap.get(tid);
        // set rules when trying to do a READ_ONLY
        // if lock already has a transaction holding it, make sure there are more
        // transaction other than itself. Don't lock self out!

        // more than one unique transaction
        if (lock.getSharedLock().size() > 1) {
            if (perm == Permissions.READ_ONLY && lock.isSharedLock()) {
                lock.addTransactionId(tid);
                transactionList.add(pageId);
            } else if (perm == Permissions.READ_WRITE) {
                // trying to do a read-write on a page with more than one transaction holding lock to page
                // just wait until page becomes available.
                wait();
            }
        } else if (lock.getSharedLock().size() == 1) { // only one transaction, make sure it is different from current
            // transaction.
            // check for different transaction
            if (!lock.getSharedLock().contains(tid)) {
                // check to see if other transaction does not have an exclusive lock to page
                if (perm == Permissions.READ_ONLY && lock.isSharedLock()) {
                    lock.addTransactionId(tid);
                    transactionList.add(pageId);
                    return;
                }
                // else, other transaction does have an exclusive lock on page, wait()
                wait();
            } else {
                // this transaction is the only one holding lock on page
                // check to see if it wants to upgrade lock.
                if (perm == Permissions.READ_WRITE) {
                    lock.setLockType(LockType.EXCLUSIVE);
                }
            }
        } else { // no transactions holding page
            if (perm == Permissions.READ_ONLY) {
                lock.setLockType(LockType.SHARED);
                lock.addTransactionId(tid);
                transactionList.add(pageId);
            } else if (perm == Permissions.READ_WRITE) {
                lock.setLockType(LockType.EXCLUSIVE);
                lock.addTransactionId(tid);
                transactionList.add(pageId);
            }
        }
    }

    /**
     * Will release all lock held by the specified transaction.
     * @param tid the TransactionID of transaction releasing pages.
     */
    public synchronized void releaseLocks(TransactionId tid) {
        // check to see if transaction holds any locks
        if (!this.transactionIdListMap.containsKey(tid)) return;
        Set<PageId> pageList = this.transactionIdListMap.get(tid);

        for (PageId pageId : pageList) {
            Lock lock = this.pageLocks.get(pageId);
            lock.removeTransactionId(tid);
            if (lock.getSharedLock().isEmpty())
                // can either remove reference to pageId or
                // set lock status to LockType.UNUSED
                lock.setLockType(LockType.UNUSED);
        }
        this.transactionIdListMap.remove(tid);
        notifyAll();
    }

    /**
     * Will release the lock the specified transaction holds on the specified page.
     * @param tid The TransactionID of the transaction releasing the lock.
     * @param pageId The PageId of the page we want to release the lock for.
     */
    public synchronized void releaseLock(TransactionId tid, PageId pageId) {
        // check to see if pageId currently has a lock on it
        if (!this.pageLocks.containsKey(pageId)) return;
        // check to see if lock has tid as a lock holder
        if (!this.pageLocks.get(pageId).getSharedLock().contains(tid)) return;
        Lock lock = this.pageLocks.get(pageId);
        lock.removeTransactionId(tid);
        if (lock.getSharedLock().isEmpty()) {
            lock.setLockType(LockType.UNUSED);
        }
        Set<PageId> pageList = this.transactionIdListMap.get(tid);
        pageList.remove(pageId);
        notifyAll();
    }

    /**
     * A check to see if a specified transaction holds a lock on the specified page.
     * @param tid The TransactionId of the transaction to check for.
     * @param pageId The PageId of the page to check for.
     * @return true when transaction does hold a lock on page, false otherwise.
     */
    public boolean holdsLock(TransactionId tid, PageId pageId) {
        return this.transactionIdListMap.get(tid).contains(pageId);
    }
}
