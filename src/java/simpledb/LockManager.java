package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class LockManager {

    //locks for page write and read
    private Map<PageId, TransactionId> writeLocks;
    private Map<PageId, Set<TransactionId>> readLocks;
    //table for pages that hold by transaction
    private Map<TransactionId, Set<PageId>> sharedPages;
    private Map<TransactionId, Set<PageId>> exclusivePages;

    public LockManager() {
        writeLocks = new ConcurrentHashMap<PageId, TransactionId>();
        readLocks = new ConcurrentHashMap<PageId, Set<TransactionId>>();
        sharedPages = new ConcurrentHashMap<TransactionId, Set<PageId>>();
        exclusivePages = new ConcurrentHashMap<TransactionId, Set<PageId>>();
    }

    //method to release transactions' locks on a page
    public synchronized void releaseLock(PageId pid, TransactionId tid) {
        Set<TransactionId> readLocksId = readLocks.get(pid);
        if (readLocksId != null && readLocksId.contains(tid)) {
            readLocksId.remove(tid);
            readLocks.put(pid, readLocksId);
        }

        if (writeLocks.containsKey(pid)) {
            writeLocks.remove(pid);
        }

        Set<PageId> sharedpagesId = sharedPages.get(tid);
        if (sharedpagesId != null && sharedpagesId.contains(pid)) {
            sharedpagesId.remove(pid);
            sharedPages.put(tid, sharedpagesId);
        }

        Set<PageId> exclusivePagesId = exclusivePages.get(tid);
        if (exclusivePagesId != null && exclusivePagesId.contains(pid)) {
            exclusivePagesId.remove(pid);
            exclusivePages.put(tid, exclusivePagesId);
        }
    }

    //check if a transaction has lock on a page or not.
    public boolean hasLocks(TransactionId tid, PageId pid){
        boolean hasWriteLock = writeLocks.get(pid).equals(tid);
        boolean hasReadLock = readLocks.get(pid).contains(tid);

        return hasWriteLock || hasReadLock;
    }

    //release all locks holds by a transaction
    public synchronized void releaseAllTransactionLocks(TransactionId tid) {
        exclusivePages.remove(tid);

        for (PageId pid : writeLocks.keySet()) {
            if (writeLocks.get(pid) == tid) {
                writeLocks.remove(pid);
            }
        }
        
        sharedPages.remove(tid);

        for (PageId pid : readLocks.keySet()) {
            Set<TransactionId> tids = readLocks.get(pid);
            if (tids != null && tids.contains(tid)) {
                tids.remove(tid);
                readLocks.put(pid, tids);
            }
        }  
    }

    //method to grant a lock on a page for a transaction 
    public synchronized boolean grantLock(PageId pid, TransactionId tid, 
            Permissions perm){
        //retrieve lock id for update.
        Set<TransactionId> readLockTid = readLocks.get(pid);
        TransactionId writeLockTid = writeLocks.get(pid);

        //handle situation with readonly permission
        if (perm.equals(Permissions.READ_ONLY)){
            if (writeLockTid == null || writeLockTid.equals(tid)) {
                if (readLockTid == null){
                    readLockTid = new HashSet<TransactionId>();
                }

                readLockTid.add(tid);
                readLocks.put(pid, readLockTid);
                Set<PageId> pageIdSet = sharedPages.get(tid);
                if (pageIdSet == null) {
                    pageIdSet = new HashSet<PageId>();
                }

                pageIdSet.add(pid);
                sharedPages.put(tid, pageIdSet);

                return false;
            } 

            return true;
        } else {//handle readwrite permission
            if ((readLockTid != null && readLockTid.size() > 1) ||
                (readLockTid != null && readLockTid.size() == 1 
                    && !readLockTid.contains(tid))) {
                return true;
            }

            if (writeLockTid == null || writeLockTid.equals(tid)) {
                writeLocks.put(pid, tid);
                Set<PageId> pidSet = exclusivePages.get(tid);
                if (pidSet == null){
                    pidSet = new HashSet<PageId>();
                }
                pidSet.add(pid);
                exclusivePages.put(tid, pidSet);

                return false;
            }

            return true;
        }
    }
}