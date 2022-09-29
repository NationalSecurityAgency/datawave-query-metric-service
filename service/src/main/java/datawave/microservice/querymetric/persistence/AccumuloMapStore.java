package datawave.microservice.querymetric.persistence;

import com.google.common.cache.CacheBuilder;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreFactory;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.MergeLockLifecycleListener;
import datawave.microservice.querymetric.QueryMetricType;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component("store")
@ConditionalOnProperty(name = "hazelcast.server.enabled")
public class AccumuloMapStore<T extends BaseQueryMetric> extends AccumuloMapLoader<T> implements MapStore<String,QueryMetricUpdate<T>> {
    
    private static AccumuloMapStore instance;
    private Logger log = LoggerFactory.getLogger(AccumuloMapStore.class);
    private IMap<Object,Object> lastWrittenQueryMetricCache;
    private MergeLockLifecycleListener mergeLock;
    private QueryMetricHandlerProperties queryMetricHandlerProperties;
    private com.google.common.cache.Cache failures;
    
    public static class Factory implements MapStoreFactory<String,BaseQueryMetric> {
        @Override
        public MapLoader<String,BaseQueryMetric> newMapStore(String mapName, Properties properties) {
            return AccumuloMapStore.instance;
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
    }
    
    @PreDestroy
    public void shutdown() {
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
    public void store(String queryId, QueryMetricUpdate<T> updatedMetricHolder) {
        long startTime = System.currentTimeMillis();
        long maxElapsedTime = this.queryMetricHandlerProperties.getMaxWriteMilliseconds();
        T updatedMetric = null;
        this.mergeLock.lock();
        try {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            
            updatedMetric = updatedMetricHolder.getMetric();
            QueryMetricType metricType = updatedMetricHolder.getMetricType();
            QueryMetricUpdate<T> lastQueryMetricUpdate = (QueryMetricUpdate<T>) lastWrittenQueryMetricCache.get(queryId);
            if (lastQueryMetricUpdate != null) {
                T lastQueryMetric = lastQueryMetricUpdate.getMetric();
                updatedMetric = handler.combineMetrics(updatedMetric, lastQueryMetric, metricType);
                // if for some reason, lastQueryMetric doesn't have lastUpdated set,
                // we can not delete the previous entries and will cause an NPE if we try
                if (lastQueryMetric.getLastUpdated() != null) {
                    final T updatedMetricFinal = updatedMetric;
                    Future<Object> future = executor.submit(() -> {
                        try {
                            handler.writeMetric(updatedMetricFinal, Collections.singletonList(lastQueryMetric), lastQueryMetric.getLastUpdated(), true);
                            return null;
                        } catch (Exception e) {
                            return e;
                        }
                    });
                    try {
                        Exception exception = (Exception) future.get(maxElapsedTime - (System.currentTimeMillis() - startTime), TimeUnit.MILLISECONDS);
                        if (exception != null) {
                            throw exception;
                        }
                    } finally {
                        future.cancel(true);
                    }
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("writing metric to accumulo: " + queryId + " - " + updatedMetricHolder.getMetric());
            } else {
                log.debug("writing metric to accumulo: " + queryId);
            }
            
            if (updatedMetric.getLastUpdated() == null) {
                updatedMetric.setLastUpdated(new Date());
            }
            
            final T updatedMetricFinal = updatedMetric;
            Future<Object> future = executor.submit(() -> {
                try {
                    handler.writeMetric(updatedMetricFinal, Collections.singletonList(updatedMetricFinal), updatedMetricFinal.getLastUpdated(), false);
                    return null;
                } catch (Exception e) {
                    return e;
                }
            });
            try {
                Exception exception = (Exception) future.get(maxElapsedTime - (System.currentTimeMillis() - startTime), TimeUnit.MILLISECONDS);
                if (exception != null) {
                    throw exception;
                }
            } finally {
                future.cancel(true);
            }
            lastWrittenQueryMetricCache.set(queryId, new QueryMetricUpdate(updatedMetric));
            failures.invalidate(queryId);
        } catch (Exception e) {
            handleException(queryId, updatedMetric, e);
        } finally {
            this.mergeLock.unlock();
        }
    }
    
    private void handleException(String queryId, T metric, Exception e) {
        if (e instanceof TimeoutException || e instanceof ExecutionException) {
            log.error("Throwing " + e.getClass().getName() + " to make Hazelcast retry store: " + e.getMessage());
        } else {
            Integer numFailures = 1;
            try {
                numFailures = (Integer) this.failures.get(queryId, () -> 0) + 1;
            } catch (ExecutionException e1) {
                log.error(e1.getMessage(), e1);
            }
            if (numFailures < 3) {
                // track the number of failures and throw an exception
                // Hazelcast will continue trying to write the failed entry
                this.failures.put(queryId, numFailures);
                throw new RuntimeException(e);
            } else {
                // stop trying by not propagating the exception
                log.error("writing metric to accumulo: " + queryId + " failed 3 times, will stop trying: " + metric, e);
                this.failures.invalidate(queryId);
            }
        }
    }
    
    @Override
    public void storeAll(Map<String,QueryMetricUpdate<T>> map) {
        Iterator<Map.Entry<String,QueryMetricUpdate<T>>> itr = map.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String,QueryMetricUpdate<T>> entry = itr.next();
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
    public void delete(String key) {
        // not implemented
    }
    
    @Override
    public void deleteAll(Collection<String> keys) {
        // not implemented
    }
}
