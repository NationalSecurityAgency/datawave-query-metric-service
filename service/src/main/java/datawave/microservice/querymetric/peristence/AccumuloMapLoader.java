package datawave.microservice.querymetric.peristence;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStoreFactory;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricUpdate;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

@Component
@ConditionalOnProperty(name = "hazelcast.server.enabled")
@Qualifier("loader")
public class AccumuloMapLoader<T extends BaseQueryMetric> implements MapLoader<String,QueryMetricUpdate<T>> {
    
    private Logger log = LoggerFactory.getLogger(getClass());
    private static AccumuloMapLoader instance;
    protected ShardTableQueryMetricHandler<T> handler;
    
    public static class Factory implements MapStoreFactory<String,QueryMetricUpdate> {
        @Override
        public MapLoader<String,QueryMetricUpdate> newMapStore(String mapName, Properties properties) {
            return AccumuloMapLoader.instance;
        }
    }
    
    @Autowired
    public AccumuloMapLoader(ShardTableQueryMetricHandler<T> handler) {
        this.handler = handler;
        AccumuloMapLoader.instance = this;
    }
    
    public static AccumuloMapLoader getInstance() {
        return instance;
    }
    
    @Override
    public QueryMetricUpdate load(String s) {
        T metric = null;
        try {
            metric = this.handler.getQueryMetric(s);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return metric == null ? null : new QueryMetricUpdate(metric);
    }
    
    @Override
    public Map<String,QueryMetricUpdate<T>> loadAll(Collection<String> keys) {
        Map<String,QueryMetricUpdate<T>> metrics = new LinkedHashMap<>();
        keys.forEach(id -> {
            BaseQueryMetric queryMetric;
            try {
                queryMetric = this.handler.getQueryMetric(id);
                if (queryMetric != null) {
                    metrics.put(id, new QueryMetricUpdate(queryMetric));
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });
        return metrics;
    }
    
    @Override
    public Iterable<String> loadAllKeys() {
        // not implemented
        return null;
    }
}
