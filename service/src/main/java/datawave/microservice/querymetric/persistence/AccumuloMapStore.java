package datawave.microservice.querymetric.persistence;

import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import com.google.common.cache.CacheBuilder;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreFactory;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.MergeLockLifecycleListener;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.microservice.querymetric.QueryMetricUpdateHolder;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.QueryMetricUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.Cache;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;

@Component("store")
@ConditionalOnProperty(name = "hazelcast.server.enabled")
public class AccumuloMapStore<T extends BaseQueryMetric> extends AccumuloMapLoader<T> implements MapStore<String,QueryMetricUpdateHolder<T>> {
    
    private static AccumuloMapStore instance;
    private Logger log = LoggerFactory.getLogger(AccumuloMapStore.class);
    private IMap<Object,Object> lastWrittenQueryMetricCache;
    private MergeLockLifecycleListener mergeLock;
    private com.google.common.cache.Cache failures;
    private Timer writeTimer = new Timer(new SlidingTimeWindowArrayReservoir(1, MINUTES));
    private boolean shuttingDown = false;
    
    public static class Factory implements MapStoreFactory<String,BaseQueryMetric> {
        @Override
        public MapLoader<String,BaseQueryMetric> newMapStore(String mapName, Properties properties) {
            return AccumuloMapStore.instance;
        }
    }
    
    @Autowired
    public AccumuloMapStore(ShardTableQueryMetricHandler handler, MergeLockLifecycleListener mergeLock) {
        this.handler = handler;
        this.mergeLock = mergeLock;
        this.failures = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();
        AccumuloMapStore.instance = this;
    }
    
    @PreDestroy
    public void shutdown() {
        this.shuttingDown = true;
        // ensure that queued updates written to the handler's
        // MultiTabletBatchWriter are flushed to Accumulo on shutdown
        try {
            this.handler.shutdown();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public void setLastWrittenQueryMetricCache(Cache lastWrittenQueryMetricCache) {
        this.lastWrittenQueryMetricCache = (IMap<Object,Object>) lastWrittenQueryMetricCache.getNativeCache();
    }
    
    @Override
    public void store(String queryId, QueryMetricUpdateHolder<T> queryMetricUpdate) {
        Timer.Context writeTimerContext = writeTimer.time();
        try {
            storeWithRetry(queryMetricUpdate);
        } finally {
            writeTimerContext.stop();
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
        T updatedMetric = null;
        this.mergeLock.lock();
        try {
            updatedMetric = (T) queryMetricUpdate.getMetric().duplicate();
            QueryMetricType metricType = queryMetricUpdate.getMetricType();
            QueryMetricUpdateHolder<T> lastQueryMetricUpdate = null;
            
            if (!queryMetricUpdate.isNewMetric()) {
                lastQueryMetricUpdate = (QueryMetricUpdateHolder<T>) lastWrittenQueryMetricCache.get(queryId);
            }
            
            if (lastQueryMetricUpdate != null) {
                T lastQueryMetric = lastQueryMetricUpdate.getMetric();
                if (metricType.equals(QueryMetricType.DISTRIBUTED)) {
                    // these values are added incrementally in a distributed update. Because we can not be sure
                    // exactly when the incomingQueryMetricCache value is stored, it would otherwise be possible
                    // for updates to be included twice.
                    updatedMetric.setSourceCount(queryMetricUpdate.getValue("sourceCount"));
                    updatedMetric.setNextCount(queryMetricUpdate.getValue("nextCount"));
                    updatedMetric.setSeekCount(queryMetricUpdate.getValue("seekCount"));
                    updatedMetric.setEvaluatedCount(queryMetricUpdate.getValue("evaluatedCount"));
                    updatedMetric.setYieldCount(queryMetricUpdate.getValue("yieldCount"));
                    updatedMetric.setDocRanges(queryMetricUpdate.getValue("docRanges"));
                    updatedMetric.setFiRanges(queryMetricUpdate.getValue("fiRanges"));
                }
                
                updatedMetric = handler.combineMetrics(updatedMetric, lastQueryMetric, metricType);
                long numUpdates = updatedMetric.getNumUpdates();
                // The createDate shouldn't change once it is set, so this is just insurance
                // We use the higher timestamp to ensure that the deletes and successive writes persist
                // As long as this timestamp is greater than when it was written, then the delete will be effective
                long deleteTimestamp;
                if (lastQueryMetric.getCreateDate().after(updatedMetric.getCreateDate())) {
                    deleteTimestamp = updatedMetric.getCreateDate().getTime() + numUpdates;
                } else {
                    deleteTimestamp = lastQueryMetric.getCreateDate().getTime() + numUpdates;
                }
                long writeTimestamp = deleteTimestamp + 1;
                updatedMetric.setNumUpdates(numUpdates + 1);
                updatedMetric.setLastUpdated(new Date(updatedMetric.getLastUpdated().getTime() + 1));
                
                if (lastQueryMetric.getLastUpdated() != null) {
                    handler.writeMetric(updatedMetric, Collections.singletonList(lastQueryMetric), deleteTimestamp, true);
                }
                handler.writeMetric(updatedMetric, Collections.singletonList(lastQueryMetric), writeTimestamp, false);
            } else {
                updatedMetric.setLastUpdated(updatedMetric.getCreateDate());
                handler.writeMetric(updatedMetric, Collections.emptyList(), updatedMetric.getCreateDate().getTime(), false);
            }
            if (log.isTraceEnabled()) {
                log.trace("writing metric to accumulo: " + queryId + " - " + queryMetricUpdate.getMetric());
            } else {
                log.debug("writing metric to accumulo: " + queryId);
            }
            
            lastWrittenQueryMetricCache.set(queryId, new QueryMetricUpdateHolder(updatedMetric));
            queryMetricUpdate.persisted();
            failures.invalidate(queryId);
        } finally {
            if (queryMetricUpdate.getMetricType().equals(QueryMetricType.DISTRIBUTED)) {
                // we've added the accumulated updates, so they can be reset
                if (updatedMetric != null) {
                    // this ensures that the incomingQueryMetricsCache has the latest values
                    queryMetricUpdate.getMetric().setSourceCount(updatedMetric.getSourceCount());
                    queryMetricUpdate.getMetric().setNextCount(updatedMetric.getNextCount());
                    queryMetricUpdate.getMetric().setSeekCount(updatedMetric.getSeekCount());
                    queryMetricUpdate.getMetric().setEvaluatedCount(updatedMetric.getEvaluatedCount());
                    queryMetricUpdate.getMetric().setYieldCount(updatedMetric.getYieldCount());
                    queryMetricUpdate.getMetric().setDocRanges(updatedMetric.getDocRanges());
                    queryMetricUpdate.getMetric().setFiRanges(updatedMetric.getFiRanges());
                }
            }
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
