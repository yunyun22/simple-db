package simpledb;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.LockSupport;

/**
 * @author wjq
 * @since 2022-03-29
 */
public class LockManager {

    private final ConcurrentHashMap<PageId, Lock> locks = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<TransactionId, Set<PageId>> transactions = new ConcurrentHashMap<>();


    public void acquireReadLock(PageId pid, TransactionId tid) {
        Lock lock = locks.computeIfAbsent(pid, key -> new Lock());
        lock.tryAcquire(tid, Lock.READ_TYPE, 0);
        Set<PageId> pageIds = transactions.computeIfAbsent(tid, key -> new CopyOnWriteArraySet<>());
        pageIds.add(pid);
    }

    public void acquireWriteLock(PageId pid, TransactionId tid) {
        Lock lock = locks.computeIfAbsent(pid, key -> new Lock());
        lock.tryAcquire(tid, Lock.WRITE_TYPE, 0);
        Set<PageId> pageIds = transactions.computeIfAbsent(tid, key -> new CopyOnWriteArraySet<>());
        pageIds.add(pid);
    }

    public boolean tryAcquireReadLock(PageId pid, TransactionId tid) {
        Lock lock = locks.computeIfAbsent(pid, key -> new Lock());
        boolean acquire = lock.acquire(tid, Lock.READ_TYPE);
        if (acquire) {
            Set<PageId> pageIds = transactions.computeIfAbsent(tid, key -> new CopyOnWriteArraySet<>());
            pageIds.add(pid);
        }
        return acquire;
    }

    public boolean tryAcquireWriteLock(PageId pid, TransactionId tid) {
        Lock lock = locks.computeIfAbsent(pid, key -> new Lock());
        boolean acquire = lock.acquire(tid, Lock.WRITE_TYPE);
        if (acquire) {
            Set<PageId> pageIds = transactions.computeIfAbsent(tid, key -> new CopyOnWriteArraySet<>());
            pageIds.add(pid);
        }
        return acquire;
    }


    public boolean holdsLock(TransactionId tid, PageId p) {
        Lock lock = locks.get(p);
        return lock.holdsLock(tid);
    }

    public void releasePage(TransactionId tid, PageId pid) {
        Lock lock = locks.get(pid);
        lock.release(tid);
    }

    public void release(TransactionId tid) {
        Set<PageId> set = transactions.get(tid);
        if (set != null && !set.isEmpty()) {
            for (PageId pageId : set) {
                releasePage(tid, pageId);
            }
        }
    }

}

final class Node {

    Node next;

    Thread thread;

    public static Node next() {
        Node node = new Node();
        node.thread = Thread.currentThread();
        return node;
    }

}

class Lock {


    private final Node head = new Node();

    private Node tail = head;

    public static final Integer WRITE_TYPE = 1;

    public static final Integer READ_TYPE = 0;

    private TransactionId writeOwner;

    private final Set<TransactionId> readOwners = new HashSet<>();


    public void tryAcquire(TransactionId tid, int type, long time) {
        long startTime = System.currentTimeMillis();
        long currentTime;
        do {
            boolean acquire = acquire(tid, type);
            if (acquire) {
                return;
            } else {
                synchronized (this) {
                    Node next = Node.next();
                    tail.next = next;
                    tail = next;
                }
            }
            LockSupport.park(time);
            currentTime = System.currentTimeMillis();
            if (currentTime - startTime < time) {
                Thread.currentThread().interrupt();
            }

        }
        while (true);
    }

    public synchronized boolean acquire(TransactionId tid, int type) {
        //当前tid存在写锁
        if (writeOwner != null) {
            return Objects.equals(tid, writeOwner);
        }

        //当前tid存在读锁
        if (!readOwners.isEmpty() && readOwners.contains(tid)) {
            if (type == READ_TYPE) {
                return true;
            }
            //锁升级
            if (type == WRITE_TYPE && readOwners.size() == 1) {
                writeOwner = tid;
                return true;
            } else {
                //升级锁失败
                return false;
            }
        }

        //读写锁互斥
        if (!readOwners.isEmpty() && type == WRITE_TYPE) {
            return false;
        }

        if (type == READ_TYPE) {
            readOwners.add(tid);
        }
        if (type == WRITE_TYPE) {
            writeOwner = tid;
        }
        return true;
    }


    public synchronized boolean holdsLock(TransactionId tid) {
        return Objects.equals(tid, this.writeOwner) || readOwners.contains(tid);
    }


    public synchronized void release(TransactionId transactionId) {
        if (Objects.equals(writeOwner, transactionId)) {
            this.writeOwner = null;
        }
        readOwners.remove(transactionId);
        Node next = head.next;
        if (next != null) {
            head.next = head.next.next;
            LockSupport.unpark(next.thread);
        }
    }
}
