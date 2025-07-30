package simpledb.transaction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

/**
* LockManager manages locks for pages in the database.
* Supports shared (read) and exclusive (write) locks with wait-for graph deadlock detection.
*/
public class LockManager {

    // Maps PageId to the set of locks held on that page
    private final ConcurrentHashMap<PageId, Set<Lock>> pageLocks;

    // Maps TransactionId to the set of locks held by that transaction
    private final ConcurrentHashMap<TransactionId, Set<Lock>> transactionLocks;

    // Maps PageId to the queue of transactions waiting for locks on that page
    private final ConcurrentHashMap<PageId, Queue<LockRequest>> waitingQueue;

    // Wait-for graph: Maps transaction to set of transactions it's waiting for
    private final ConcurrentHashMap<TransactionId, Set<TransactionId>> waitForGraph;

    public LockManager() {
        this.pageLocks = new ConcurrentHashMap<>();
        this.transactionLocks = new ConcurrentHashMap<>();
        this.waitingQueue = new ConcurrentHashMap<>();
        this.waitForGraph = new ConcurrentHashMap<>();
    }

    /**
     * Represents a lock held by a transaction on a page
     */
    private static class Lock {
        final TransactionId tid;
        final PageId pid;
        final Permissions perm;

        Lock(TransactionId tid, PageId pid, Permissions perm) {
            this.tid = tid;
            this.pid = pid;
            this.perm = perm;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Lock)) return false;
            Lock lock = (Lock) o;
            return Objects.equals(tid, lock.tid) && 
                   Objects.equals(pid, lock.pid) && 
                   perm == lock.perm;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tid, pid, perm);
        }

        @Override
        public String toString() {
            return String.format("Lock{tid=%s, pid=%s, perm=%s}", tid, pid, perm);
        }
    }

    /**
     * Represents a lock request waiting to be granted
     */
    private static class LockRequest {
        final TransactionId tid;
        final PageId pid;
        final Permissions perm;
        final long timestamp;

        LockRequest(TransactionId tid, PageId pid, Permissions perm) {
            this.tid = tid;
            this.pid = pid;
            this.perm = perm;
            this.timestamp = System.currentTimeMillis();
        }
    }

    /**
     * Attempts to acquire a lock for the given transaction on the specified page.
     * Uses wait-for graph cycle detection to prevent deadlocks.
     * 
     * @param tid Transaction requesting the lock
     * @param pid Page to lock
     * @param perm Permission type (READ_ONLY or READ_WRITE)
     * @throws TransactionAbortedException if deadlock is detected
     */
    public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm) 
    throws TransactionAbortedException{

    // If already holds the needed or stronger lock, do nothing
    if (holdsLock(tid, pid, perm) || 
        (perm == Permissions.READ_ONLY && holdsLock(tid, pid, Permissions.READ_WRITE))) {
        return;
    }

    // Check if we can grant the lock immediately
    if (canGrantLock(tid, pid, perm)) {
        grantLock(tid, pid, perm);
        return;
    }

    // Cannot grant lock right now — collect blocking TIDs and check for deadlock
    Set<TransactionId> blockingTransactions = getBlockingTransactions(tid, pid, perm);
    waitForGraph.put(tid, blockingTransactions);

    if (hasCycle()) {
        waitForGraph.remove(tid);
        throw new TransactionAbortedException("");
    }

    // Enqueue the request
    waitingQueue.computeIfAbsent(pid, k -> new LinkedList<>()).offer(new LockRequest(tid, pid, perm));

    // Block until we are at the front of the queue AND the lock can be granted
    while (!canGrantLock(tid, pid, perm) || !isFirstInQueue(tid, pid)) {
        try {
            wait(50);
        } catch (InterruptedException e) {
            cleanupWaitingTransaction(tid, pid);
            Thread.currentThread().interrupt();
            throw new TransactionAbortedException("");
        }

        // Optional: Deadlock check again during long waits
        if (hasCycle()) {
            cleanupWaitingTransaction(tid, pid);
            throw new TransactionAbortedException("");
        }
    }

    // Lock can now be granted
    cleanupWaitingTransaction(tid, pid);
    grantLock(tid, pid, perm);
    notifyAll(); // Wake up other waiting transactions
}
private boolean isFirstInQueue(TransactionId tid, PageId pid) {
    Queue<LockRequest> queue = waitingQueue.get(pid);
    return queue != null && !queue.isEmpty() && queue.peek().tid.equals(tid);
}


    /**
     * Gets the set of transactions that are blocking the requested lock
     */
    private Set<TransactionId> getBlockingTransactions(TransactionId tid, PageId pid, Permissions perm) {
        Set<TransactionId> blocking = new HashSet<>();
        Set<Lock> locks = pageLocks.get(pid);

        if (locks == null || locks.isEmpty()) {
            return blocking;
        }

        if (perm == Permissions.READ_WRITE) {
            // Write lock is blocked by any existing lock (except own read lock for upgrade)
            for (Lock lock : locks) {
                if (!lock.tid.equals(tid)) {
                    blocking.add(lock.tid);
                }
            }
        } else {
            // Read lock is blocked by write locks only
            for (Lock lock : locks) {
                if (lock.perm == Permissions.READ_WRITE) {
                    blocking.add(lock.tid);
                }
            }
        }

        return blocking;
    }

    /**
     * Detects cycles in the wait-for graph using DFS
     */
    private boolean hasCycle() {
        Set<TransactionId> visited = new HashSet<>();
        Set<TransactionId> recursionStack = new HashSet<>();

        for (TransactionId tid : waitForGraph.keySet()) {
            if (!visited.contains(tid)) {
                if (hasCycleDFS(tid, visited, recursionStack)) {
                    return true;
                }
            }
        }
        return false;
    }

 
    /**
     * DFS helper for cycle detection
     */
    private boolean hasCycleDFS(TransactionId tid, Set<TransactionId> visited, Set<TransactionId> recursionStack) {
        visited.add(tid);
        recursionStack.add(tid);

        Set<TransactionId> neighbors = waitForGraph.get(tid);
        if (neighbors != null) {
            for (TransactionId neighbor : neighbors) {
                if (!visited.contains(neighbor)) {
                    if (hasCycleDFS(neighbor, visited, recursionStack)) {
                        return true;
                    }
                } else if (recursionStack.contains(neighbor)) {
                    return true; // Back edge found, cycle detected
                }
            }
        }

        recursionStack.remove(tid);
        return false;
    }

    /**
     * Cleans up waiting transaction from all data structures
     */
    private void cleanupWaitingTransaction(TransactionId tid, PageId pid) {
        // Remove from waiting queue
        Queue<LockRequest> queue = waitingQueue.get(pid);
        if (queue != null) {
            queue.removeIf(req -> req.tid.equals(tid));
            if (queue.isEmpty()) {
                waitingQueue.remove(pid);
            }
        }

        // Remove from wait-for graph
        waitForGraph.remove(tid);
    }

    /**
     * Checks if a transaction can be granted a lock on a page
     */
    private boolean canGrantLock(TransactionId tid, PageId pid, Permissions perm) {
        Set<Lock> locks = pageLocks.get(pid);
        if (locks == null || locks.isEmpty()) {
            return true; // No locks on this page
        }

        // Check if requesting write lock
        if (perm == Permissions.READ_WRITE) {
            // Write lock can only be granted if no other locks exist,
            // or if this transaction holds the only read lock (upgrade case)
            if (locks.size() == 1) {
                Lock existingLock = locks.iterator().next();
                return existingLock.tid.equals(tid) && existingLock.perm == Permissions.READ_ONLY;
            }
            return locks.isEmpty();
        } else {
            // Read lock can be granted if no write locks exist
            return locks.stream().noneMatch(lock -> lock.perm == Permissions.READ_WRITE);
        }
    }

    /**
     * Grants a lock to a transaction
     */
    private void grantLock(TransactionId tid, PageId pid, Permissions perm) {
        // If upgrading from read to write lock, remove the read lock first
        if (perm == Permissions.READ_WRITE && holdsLock(tid, pid, Permissions.READ_ONLY)) {
            releaseLock(tid, pid, Permissions.READ_ONLY);
        }

        Lock lock = new Lock(tid, pid, perm);

        pageLocks.computeIfAbsent(pid, k -> ConcurrentHashMap.newKeySet()).add(lock);
        transactionLocks.computeIfAbsent(tid, k -> ConcurrentHashMap.newKeySet()).add(lock);
    }

    /**
     * Checks if a transaction holds a specific lock on a page
     * Note: A write lock implies read permissions
     */
    public synchronized boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        Set<Lock> locks = transactionLocks.get(tid);
        if (locks == null) return false;

        // Check for exact lock match
        if (locks.contains(new Lock(tid, pid, perm))) {
            return true;
        }

        // If requesting read lock, check if we hold write lock (write implies read)
        if (perm == Permissions.READ_ONLY) {
            return locks.contains(new Lock(tid, pid, Permissions.READ_WRITE));
        }

        return false;
    }

    /**
     * Checks if a transaction holds any lock on a page
     */
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        Set<Lock> locks = transactionLocks.get(tid);
        if (locks == null) return false;

        return locks.stream().anyMatch(lock -> lock.pid.equals(pid));
    }

    /**
     * Releases a specific lock held by a transaction
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid, Permissions perm) {
        Lock lock = new Lock(tid, pid, perm);

        Set<Lock> pageLockSet = pageLocks.get(pid);
        if (pageLockSet != null) {
            pageLockSet.remove(lock);
            if (pageLockSet.isEmpty()) {
                pageLocks.remove(pid);
            }
        }

        Set<Lock> transactionLockSet = transactionLocks.get(tid);
        if (transactionLockSet != null) {
            transactionLockSet.remove(lock);
            if (transactionLockSet.isEmpty()) {
                transactionLocks.remove(tid);
            }
        }

        // Update wait-for graph for transactions that might now be unblocked
        updateWaitForGraph();

        notifyAll(); // Notify waiting transactions
    }

    /**
     * Updates the wait-for graph after a lock is released
     */
    private void updateWaitForGraph() {
        // Recalculate wait-for relationships for all waiting transactions
        Map<TransactionId, Set<TransactionId>> newWaitForGraph = new HashMap<>();

        for (Map.Entry<PageId, Queue<LockRequest>> entry : waitingQueue.entrySet()) {
            PageId pid = entry.getKey();
            for (LockRequest request : entry.getValue()) {
                Set<TransactionId> blocking = getBlockingTransactions(request.tid, pid, request.perm);
                if (!blocking.isEmpty()) {
                    newWaitForGraph.put(request.tid, blocking);
                }
            }
        }

        waitForGraph.clear();
        waitForGraph.putAll(newWaitForGraph);
    }

    /**
     * Releases all locks held by a transaction
     */
    public synchronized void releaseAllLocks(TransactionId tid) {
        Set<Lock> locks = transactionLocks.get(tid);
        if (locks == null) return;

        // Create a copy to avoid concurrent modification
        Set<Lock> locksCopy = new HashSet<>(locks);
        for (Lock lock : locksCopy) {
            releaseLock(tid, lock.pid, lock.perm);
        }

        transactionLocks.remove(tid);
        waitForGraph.remove(tid); // Clean up wait-for graph

        // Remove from all waiting queues
        for (Queue<LockRequest> queue : waitingQueue.values()) {
            queue.removeIf(req -> req.tid.equals(tid));
        }

        notifyAll(); // Notify waiting transactions
    }

    /**
     * Gets debug information about current lock state
     */
    public synchronized String getDebugInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Lock Manager Debug Info ===\n");
        sb.append("Page Locks:\n");
        for (Map.Entry<PageId, Set<Lock>> entry : pageLocks.entrySet()) {
            sb.append(String.format("  Page %s: %s\n", entry.getKey(), entry.getValue()));
        }
        sb.append("Transaction Locks:\n");
        for (Map.Entry<TransactionId, Set<Lock>> entry : transactionLocks.entrySet()) {
            sb.append(String.format("  Transaction %s: %s\n", entry.getKey(), entry.getValue()));
        }
        sb.append("Wait-For Graph:\n");
        for (Map.Entry<TransactionId, Set<TransactionId>> entry : waitForGraph.entrySet()) {
            sb.append(String.format("  %s waits for: %s\n", entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }
}
 