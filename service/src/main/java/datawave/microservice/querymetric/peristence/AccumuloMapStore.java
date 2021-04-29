package datawave.microservice.querymetric.peristence;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreFactory;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.BaseQueryMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.Cache;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@Component
@ConditionalOnProperty(name = "hazelcast.server.enabled")
@Qualifier("store")
public class AccumuloMapStore<T extends BaseQueryMetric> extends AccumuloMapLoader<T> implements MapStore<String,T> {
    
    private static AccumuloMapStore instance;
    private Logger log = LoggerFactory.getLogger(getClass());
    private MapProxyImpl lastWrittenQueryMetricCache;
    
    public static class Factory implements MapStoreFactory<String,BaseQueryMetric> {
        @Override
        public MapLoader<String,BaseQueryMetric> newMapStore(String mapName, Properties properties) {
            return AccumuloMapStore.instance;
        }
    }
    
    @Autowired
    public AccumuloMapStore(ShardTableQueryMetricHandler handler) {
        super(handler);
        AccumuloMapStore.instance = this;
    }
    
    public void setLastWrittenQueryMetricCache(Cache lastWrittenQueryMetricCache) {
        this.lastWrittenQueryMetricCache = (MapProxyImpl) lastWrittenQueryMetricCache.getNativeCache();
    }
    
    @Override
    public void store(String queryId, T updatedMetric) {
        lastWrittenQueryMetricCache.lock(queryId);
        try {
            T lastQueryMetric = (T) lastWrittenQueryMetricCache.get(queryId);
            if (lastQueryMetric != null) {
                updatedMetric = handler.combineMetrics(updatedMetric, lastQueryMetric);
                handler.writeMetric(updatedMetric, Collections.singletonList(lastQueryMetric), lastQueryMetric.getLastUpdated(), true);
            }
            handler.writeMetric(updatedMetric, Collections.singletonList(updatedMetric), updatedMetric.getLastUpdated(), false);
            lastWrittenQueryMetricCache.set(queryId, updatedMetric);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            lastWrittenQueryMetricCache.unlock(queryId);
        }
    }
    
    @Override
    public void storeAll(Map<String,T> map) {
        map.forEach((queryId, updatedMetric) -> {
            try {
                this.store(queryId, updatedMetric);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });
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
