package datawave.microservice.querymetric.persistence;

import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreFactory;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.MergeLockLifecycleListener;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.microservice.querymetric.QueryMetricUpdateHolder;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.QueryMetricUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.Cache;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@Component("store")
@ConditionalOnProperty(name = "hazelcast.server.enabled")
public class AccumuloMapStore<T extends BaseQueryMetric> extends AccumuloMapLoader<T> implements MapStore<String,QueryMetricUpdateHolder<T>> {
    
    private static AccumuloMapStore instance;
    private Logger log = LoggerFactory.getLogger(AccumuloMapStore.class);
    private IMap<Object,Object> lastWrittenQueryMetricCache;
    private MergeLockLifecycleListener mergeLock;
    private QueryMetricHandlerProperties queryMetricHandlerProperties;
    private com.google.common.cache.Cache failures;
    private ExecutorService executorService;
    private SynchronousQueue<QueryMetricUpdateHolder> updateQueue = new SynchronousQueue<>();
    private List<Writer> updateWriters = new ArrayList<>();
    private Timer writeTimer = new Timer(new SlidingTimeWindowArrayReservoir(1, MINUTES));
    private boolean shuttingDown = false;
    
    public static class Factory implements MapStoreFactory<String,BaseQueryMetric> {
        @Override
        public MapLoader<String,BaseQueryMetric> newMapStore(String mapName, Properties properties) {
            return AccumuloMapStore.instance;
        }
    }
    
    public class Writer implements Runnable {
        private SynchronousQueue<QueryMetricUpdateHolder> updateQueue;
        private boolean shuttingDown = false;
        
        public Writer(SynchronousQueue<QueryMetricUpdateHolder> updateQueue) {
            this.updateQueue = updateQueue;
        }
        
        public void setShuttingDown(boolean shuttingDown) {
            this.shuttingDown = shuttingDown;
        }
        
        @Override
        public void run() {
            while (!this.shuttingDown) {
                try {
                    QueryMetricUpdateHolder queryMetricUpdate = this.updateQueue.take();
                    if (queryMetricUpdate != null) {
                        Timer.Context writeTimerContext = writeTimer.time();
                        try {
                            AccumuloMapStore.this.storeWithRetry(queryMetricUpdate);
                        } finally {
                            writeTimerContext.stop();
                        }
                    }
                } catch (InterruptedException e) {
                    if (!this.shuttingDown) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }
    }
    
    @Autowired
    public AccumuloMapStore(ShardTableQueryMetricHandler handler, QueryMetricHandlerProperties queryMetricHandlerProperties,
                    MergeLockLifecycleListener mergeLock) {
        this.handler = handler;
        this.mergeLock = mergeLock;
        this.queryMetricHandlerProperties = queryMetricHandlerProperties;
        this.failures = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();
        AccumuloMapStore.instance = this;
        int writerThreads = queryMetricHandlerProperties.getMapStoreWriteThreads();
        this.executorService = Executors.newFixedThreadPool(writerThreads, new ThreadFactoryBuilder().setNameFormat("map-store-write-thread-%d").build());
        if (writerThreads > 1) {
            for (int x = 0; x < writerThreads; x++) {
                Writer w = new Writer(this.updateQueue);
                this.updateWriters.add(w);
                this.executorService.submit(w);
            }
        }
    }
    
    @PreDestroy
    public void shutdown() {
        this.shuttingDown = true;
        // stop the writer threads
        for (Writer w : this.updateWriters) {
            w.setShuttingDown(true);
        }
        // ensure that queued updates written to the handler's
        // MultiTabletBatchWriter are flushed to Accumulo on shutdown
        try {
            this.handler.shutdown();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        // ensure the writer threads exit
        boolean shutdownSuccess = false;
        try {
            shutdownSuccess = this.executorService.awaitTermination(60, SECONDS);
        } catch (InterruptedException e) {
            
        }
        if (!shutdownSuccess) {
            this.executorService.shutdownNow();
        }
    }
    
    public void setLastWrittenQueryMetricCache(Cache lastWrittenQueryMetricCache) {
        this.lastWrittenQueryMetricCache = (IMap<Object,Object>) lastWrittenQueryMetricCache.getNativeCache();
    }
    
    @Override
    public void store(String queryId, QueryMetricUpdateHolder<T> queryMetricUpdate) {
        if (queryMetricHandlerProperties.getMapStoreWriteThreads() > 1) {
            try {
                this.updateQueue.put(queryMetricUpdate);
            } catch (Exception e) {
                // hazelcast will retry storing the update
                throw new RuntimeException(e);
            }
        } else {
            storeWithRetry(queryMetricUpdate);
        }
    }
    
    public void storeWithRetry(QueryMetricUpdateHolder<T> queryMetricUpdate) {
        boolean retry = true;
        boolean success = false;
        while (!this.shuttingDown && !success && retry) {
            try {
                store(queryMetricUpdate);
                success = true;
            } catch (Exception e) {
                if (!this.shuttingDown) {
                    retry = AccumuloMapStore.this.retryOnException(queryMetricUpdate, e);
                    if (retry) {
                        log.info("retrying store of {}", queryMetricUpdate.getMetric().getQueryId());
                    }
                }
            }
        }
    }
    
    public void store(QueryMetricUpdateHolder<T> queryMetricUpdate) throws Exception {
        String queryId = queryMetricUpdate.getMetric().getQueryId();
        T updatedMetric;
        this.mergeLock.lock();
        try {
            updatedMetric = queryMetricUpdate.getMetric();
            QueryMetricType metricType = queryMetricUpdate.getMetricType();
            QueryMetricUpdateHolder<T> lastQueryMetricUpdate = null;
            if (!queryMetricUpdate.isNewMetric()) {
                lastQueryMetricUpdate = (QueryMetricUpdateHolder<T>) lastWrittenQueryMetricCache.get(queryId);
            }
            
            if (lastQueryMetricUpdate != null) {
                T lastQueryMetric = lastQueryMetricUpdate.getMetric();
                updatedMetric = handler.combineMetrics(updatedMetric, lastQueryMetric, metricType);
                // if for some reason, lastQueryMetric doesn't have lastUpdated set,
                // we can not delete the previous entries and will cause an NPE if we try
                if (lastQueryMetric.getLastUpdated() != null) {
                    handler.writeMetric(updatedMetric, Collections.singletonList(lastQueryMetric), lastQueryMetric.getLastUpdated(), true);
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("writing metric to accumulo: " + queryId + " - " + queryMetricUpdate.getMetric());
            } else {
                log.debug("writing metric to accumulo: " + queryId);
            }
            
            if (updatedMetric.getLastUpdated() == null) {
                updatedMetric.setLastUpdated(new Date());
            }
            
            handler.writeMetric(updatedMetric, Collections.singletonList(updatedMetric), updatedMetric.getLastUpdated(), false);
            lastWrittenQueryMetricCache.set(queryId, new QueryMetricUpdateHolder(updatedMetric));
            failures.invalidate(queryId);
        } finally {
            this.mergeLock.unlock();
        }
    }
    
    private boolean retryOnException(QueryMetricUpdate update, Exception e) {
        String queryId = update.getMetric().getQueryId();
        Integer numFailures = 1;
        try {
            numFailures = (Integer) this.failures.get(queryId, () -> 0) + 1;
        } catch (ExecutionException e1) {
            log.error(e1.getMessage(), e1);
        }
        if (numFailures < 3) {
            // track the number of failures and throw an exception
            // so that we will continue trying to write the failed entry
            this.failures.put(queryId, numFailures);
            return true;
        } else {
            // stop trying by not propagating the exception
            log.error("writing metric to accumulo: " + queryId + " failed 3 times, will stop trying: " + update.getMetric(), e);
            this.failures.invalidate(queryId);
            return false;
        }
    }
    
    @Override
    public void storeAll(Map<String,QueryMetricUpdateHolder<T>> map) {
        Iterator<Map.Entry<String,QueryMetricUpdateHolder<T>>> itr = map.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String,QueryMetricUpdateHolder<T>> entry = itr.next();
            try {
                this.store(entry.getKey(), entry.getValue());
                // remove entries that succeeded so that a potential
                // subsequent failure will know which updates remain
                itr.remove();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }
    
    @Override
    public QueryMetricUpdateHolder load(String s) {
        return null;
    }
    
    @Override
    public Map<String,QueryMetricUpdateHolder<T>> loadAll(Collection<String> keys) {
        return null;
    }
    
    @Override
    public void delete(String key) {
        // not implemented
    }
    
    @Override
    public void deleteAll(Collection<String> keys) {
        // not implemented
    }
    
    public Timer getWriteTimer() {
        return writeTimer;
    }
}
