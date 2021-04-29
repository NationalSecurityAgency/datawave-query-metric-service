package datawave.microservice.querymetric.peristence;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStoreFactory;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.BaseQueryMetric;
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
public class AccumuloMapLoader<T extends BaseQueryMetric> implements MapLoader<String,T> {
    
    private Logger log = LoggerFactory.getLogger(getClass());
    private static AccumuloMapLoader instance;
    protected ShardTableQueryMetricHandler<T> handler;
    
    public static class Factory implements MapStoreFactory<String,BaseQueryMetric> {
        @Override
        public MapLoader<String,BaseQueryMetric> newMapStore(String mapName, Properties properties) {
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
    public T load(String s) {
        return this.handler.getQueryMetric(s);
    }
    
    @Override
    public Map<String,T> loadAll(Collection<String> keys) {
        Map<String,T> metrics = new LinkedHashMap<>();
        keys.forEach(id -> {
            T queryMetric = this.handler.getQueryMetric(id);
            if (queryMetric != null) {
                metrics.put(id, queryMetric);
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
