package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
        private Set<TransactionId> waitingTid;

        public Lock(LockType type) {
            this.type = type;
            this.sharedLock = new HashSet<>();
            this.waitingTid = new HashSet<>();
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

        public void addWaitingTid(TransactionId tid) {
            this.waitingTid.add(tid);
        }

        public void removeWaitingTid(TransactionId tid) {
            this.waitingTid.remove(tid);
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
    private Map<TransactionId, Set<TransactionId>> dependencyGraph;

    /**
     * Constructs a LockManager with empty fields.
     */
    public LockManager() {
        this.pageLocks = new ConcurrentHashMap<>();
        this.transactionIdListMap = new ConcurrentHashMap<>();
        this.dependencyGraph = new ConcurrentHashMap<>();
    }

    /**
     * Used for checking for deadlocks. For all TransactionIds the current transaction (tid) is waiting on, check to
     * see that none of the those transactions are waiting on it.
     * @param tid the TransactionId to check against.
     * @throws TransactionAbortedException if a deadlock is detected.
     */
    private synchronized void checkDeadLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        Lock lock = this.pageLocks.get(pid);
        Set<TransactionId> visited = new HashSet<>();
        Queue<TransactionId> queue = new ArrayDeque<>(lock.getSharedLock());
        while(!queue.isEmpty()) {
            TransactionId currentTid = queue.poll();
            Set<TransactionId> waitingFor = this.dependencyGraph.get(currentTid);
            if (waitingFor == null) return;
            for (TransactionId id : waitingFor) {
                if (!visited.contains(id)) {
                    if (id.equals(tid)) {
                        throw new TransactionAbortedException();
                    }
                    queue.add(id);
                    visited.add(id);
                }
            }
        }
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
    public synchronized void acquireLock(TransactionId tid, PageId pageId, Permissions perm) throws InterruptedException, TransactionAbortedException {
        this.pageLocks.putIfAbsent(pageId, new Lock(LockType.UNUSED));
        this.transactionIdListMap.putIfAbsent(tid, new HashSet<>());
        this.dependencyGraph.putIfAbsent(tid, new HashSet<>());

        Lock lock = this.pageLocks.get(pageId);
        Set<PageId> transactionList = this.transactionIdListMap.get(tid);

        while(true) {
            // more than one unique transaction
            if (lock.getSharedLock().size() > 1) {
                // trying to do a read on a page with more than one transaction holding lock to page
                if (perm == Permissions.READ_ONLY && lock.isSharedLock()) {
                    lock.addTransactionId(tid);
                    lock.removeWaitingTid(tid);
                    transactionList.add(pageId);
                    return;
                } else {
                    // trying to do a read-write to a page that already has other transactions holding a lock.
                    // add self to waiting list.
                    lock.addWaitingTid(tid);
                    this.dependencyGraph.get(tid).addAll(lock.getSharedLock());
                }
            } else if (lock.getSharedLock().size() == 1) {
                // only one transaction, make sure it is different from current transaction.
                // check to see if this transaction is the only one holding lock on page
                if (lock.getSharedLock().contains(tid)) {
                    // check if we need to update lock status
                    if (perm == Permissions.READ_WRITE)
                        lock.setLockType(LockType.EXCLUSIVE);
                    lock.removeWaitingTid(tid);
                    return;
                } else { // another transaction holds a lock to page
                    if (perm == Permissions.READ_ONLY && lock.isSharedLock()) {
                        lock.addTransactionId(tid);
                        transactionList.add(pageId);
                        return;
                    } else {
                        this.dependencyGraph.get(tid).addAll(lock.getSharedLock());
                    }
                }
            } else { // no transactions holding page
                if (perm == Permissions.READ_ONLY) {
                    lock.setLockType(LockType.SHARED);
                } else {
                    lock.setLockType(LockType.EXCLUSIVE);
                }
                lock.addTransactionId(tid);
                lock.removeWaitingTid(tid);
                transactionList.add(pageId);
                return;
            }
            // current transaction was not able to gain access, wait until lock becomes available
            checkDeadLock(tid, pageId);
            wait();
        }
    }

    /**
     * Will release all lock held by the specified transaction.
     * @param tid the TransactionId of transaction releasing pages.
     */
    public synchronized void releaseLocks(TransactionId tid) {
        // check to see if transaction holds any locks
        if (!this.transactionIdListMap.containsKey(tid)) return;
        Set<PageId> pageList = this.transactionIdListMap.get(tid);

        for (PageId pageId : pageList) {
            Lock lock = this.pageLocks.get(pageId);
            lock.removeTransactionId(tid);
            lock.removeWaitingTid(tid);
            if (lock.getSharedLock().isEmpty()) {
                lock.setLockType(LockType.UNUSED);
            }
        }

        for (Set<TransactionId> set : this.dependencyGraph.values()) {
            set.remove(tid);
        }

        this.transactionIdListMap.remove(tid);
        this.dependencyGraph.remove(tid);
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

        for (TransactionId id : lock.getSharedLock()) {
            Set<TransactionId> waitingSet = this.dependencyGraph.get(id);
            if (waitingSet != null) {
                waitingSet.remove(tid);
            }
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
        Set<PageId> pageList = this.transactionIdListMap.get(tid);
        if (pageList != null)
            return pageList.contains(pageId);
        return false;
    }

    public Set<PageId> getTransactionPageList(TransactionId tid) {
        return this.transactionIdListMap.get(tid);
    }
}
