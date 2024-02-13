package simpledb;

import java.util.*;

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
            return this.type == LockType.UNUSED || this.type == LockType.SHARED;
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

    public LockManager() {
        this.pageLocks = new HashMap<>();
        this.transactionIdListMap = new HashMap<>();
    }

    /**
     * Transaction will try to acquire a lock on a page.
     * @param tid TransactionId of transaction trying to acquire the lock.
     * @param pageId PageId of the page the transaction is trying to acquire the lock for.
     * @param perm Permission the transaction is asking for (Read/Write)
     * @return true on success and false on failure
     */
    public synchronized void acquireLock(TransactionId tid, PageId pageId, Permissions perm) throws InterruptedException {
        this.pageLocks.putIfAbsent(pageId, new Lock(LockType.UNUSED));
        this.transactionIdListMap.putIfAbsent(tid, new HashSet<>());

        Lock lock = this.pageLocks.get(pageId);
        Set<PageId> transactionList = this.transactionIdListMap.get(tid);
        if (perm == Permissions.READ_ONLY && lock.isSharedLock()) {
            lock.setLockType(LockType.SHARED);
            lock.addTransactionId(tid);
            transactionList.add(pageId);
            notifyAll();
        } else if (perm == Permissions.READ_WRITE && canAcquireExclusiveLock(tid, pageId)) {
            lock.setLockType(LockType.EXCLUSIVE);
            lock.addTransactionId(tid);
            transactionList.add(pageId);
            notifyAll();
        }
        wait();
    }

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

    public boolean holdsLock(TransactionId tid, PageId pageId) {
        return this.transactionIdListMap.get(tid).contains(pageId);
    }

    /**
     *  Helper method to check if a transaction can acquire an exclusive lock
     * @param tid TransactionId of the Transaction trying to acquire the lock
     * @param pageId PageId of the page the transaction is trying to acquire the lock for
     * @return true or false depending on whether page is not currently held by another transaction
     */
    private boolean canAcquireExclusiveLock(TransactionId tid, PageId pageId) {
        Lock lock = this.pageLocks.get(pageId);
        return lock.type == LockType.UNUSED || (lock.getSharedLock().size() == 1 && lock.getSharedLock().contains(tid));
    }

}
