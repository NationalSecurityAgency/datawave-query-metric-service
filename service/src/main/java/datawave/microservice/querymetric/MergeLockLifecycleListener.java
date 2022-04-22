package datawave.microservice.querymetric;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MergeLockLifecycleListener implements LifecycleListener, HazelcastInstanceAware {
    
    private static Logger log = LoggerFactory.getLogger(MergeLockLifecycleListener.class);
    public WriteLockRunnable writeLockRunnable;
    private ReentrantReadWriteLock clusterLock;
    private Future writeLockRunnableFuture;
    private HazelcastInstance instance;
    private String localMemberUuid;
    
    public MergeLockLifecycleListener() {
        this.clusterLock = new ReentrantReadWriteLock(true);
        this.writeLockRunnable = new WriteLockRunnable(this.clusterLock);
        this.writeLockRunnableFuture = Executors.newSingleThreadExecutor().submit(this.writeLockRunnable);
    }
    
    @PreDestroy
    public void preDestroy() {
        this.writeLockRunnable.shutdown();
        try {
            this.writeLockRunnableFuture.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            this.writeLockRunnableFuture.cancel(true);
        }
    }
    
    @Override
    public void setHazelcastInstance(HazelcastInstance instance) {
        this.instance = instance;
    }
    
    private String getLocalMemberUuid() {
        try {
            if (this.instance != null) {
                this.localMemberUuid = this.instance.getCluster().getLocalMember().getUuid();
            }
        } catch (HazelcastInstanceNotActiveException e) {
            // this.instance.getCluster() will throw an exception during shutdown
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return (this.localMemberUuid == null) ? "" : this.localMemberUuid;
    }
    
    // A writeLock around STARTING and STARTED is taken care of in HazelcastMetricCacheConfiguration.hazelcastInstance to
    // ensure that the lastWrittenQueryMetricCache is set into the MapStore before the instance is active and the writeLock is released
    @Override
    public void stateChanged(LifecycleEvent event) {
        log.info(event.toString() + " [" + getLocalMemberUuid() + "]");
        switch (event.getState()) {
            case MERGING:
                // lock for a maximum time so that we don't lock forever
                this.writeLockRunnable.lock(120000);
                break;
            case SHUTTING_DOWN:
                // lock for a maximum time so that we don't lock forever
                this.writeLockRunnable.lock(60000);
                break;
            case MERGED:
            case MERGE_FAILED:
            case SHUTDOWN:
                this.writeLockRunnable.unlock();
                break;
        }
    }
    
    public void lock() {
        if (log.isTraceEnabled()) {
            log.trace("locking for read");
        }
        clusterLock.readLock().lock();
        if (log.isTraceEnabled()) {
            log.trace("locked for read");
        }
    }
    
    public void unlock() {
        if (log.isTraceEnabled()) {
            log.trace("unlocking for read");
        }
        clusterLock.readLock().unlock();
        if (log.isTraceEnabled()) {
            log.trace("unlocked for read");
        }
    }
    
    public class WriteLockRunnable implements Runnable {
        
        private ReentrantReadWriteLock clusterLock;
        private AtomicBoolean requestLock = new AtomicBoolean();
        private AtomicBoolean requestUnlock = new AtomicBoolean();
        private long scheduledUnlockTime = Long.MAX_VALUE;
        private boolean shuttingDown = false;
        
        public WriteLockRunnable(ReentrantReadWriteLock clusterLock) {
            this.clusterLock = clusterLock;
        }
        
        @Override
        public void run() {
            while (!shuttingDown) {
                if (this.requestLock.get() == true) {
                    this.clusterLock.writeLock().lock();
                    // allow the lock() thread to continue
                    this.requestLock.set(false);
                    try {
                        // wait for unlock() to be called or a scheduled unlock
                        while (requestUnlock.get() == false && System.currentTimeMillis() < this.scheduledUnlockTime && !shuttingDown) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                log.error(e.getMessage(), e);
                            }
                        }
                    } finally {
                        this.clusterLock.writeLock().unlock();
                        // allow the unlock() thread to continue
                        this.requestUnlock.set(false);
                    }
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }
        
        public void lock(long maxLockMilliseconds) {
            log.info("locking for write");
            // prompt run() method to lock the writeLock
            if (this.requestLock.compareAndSet(false, true)) {
                this.scheduledUnlockTime = System.currentTimeMillis() + maxLockMilliseconds;
                // wait until run() method locks the writeLock and sets requestLock to false
                while (requestLock.get() == true) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
            log.info("locked for write");
        }
        
        public void unlock() {
            log.info("unlocking for write");
            // cancel any scheduled unlock
            this.scheduledUnlockTime = Long.MAX_VALUE;
            // prompt run() method to unlock the writeLock
            if (this.requestUnlock.compareAndSet(false, true)) {
                // wait until run() method unlocks the writeLock and sets requestUnlock to false
                while (requestUnlock.get() == true) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
            log.info("unlocked for write");
        }
        
        public void shutdown() {
            this.shuttingDown = true;
        }
    }
}
