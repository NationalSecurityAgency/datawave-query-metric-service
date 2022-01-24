package datawave.microservice.querymetric.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "hazelcast.server.enabled", havingValue = "false")
public class CaffeineMetricCacheConfiguration {
    
    @Bean(name = "queryMetricCacheManager")
    public CaffeineCacheManager queryMetricCacheManager() {
        return new CaffeineCacheManager();
    }
}
