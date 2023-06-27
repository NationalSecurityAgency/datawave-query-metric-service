package datawave.microservice.querymetric.config;

import javax.inject.Named;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.querymetric.QueryMetricOperationsStats;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.persistence.AccumuloMapStore;

@Configuration
public class StatsConfiguration {
    
    @Bean
    @ConditionalOnMissingBean
    QueryMetricOperationsStats queryMetricOperationsStats(TimelyProperties timelyProperties, ShardTableQueryMetricHandler handler,
                    @Named("queryMetricCacheManager") CacheManager cacheManager, AccumuloMapStore mapStore) {
        return new QueryMetricOperationsStats(timelyProperties, handler, cacheManager, mapStore);
    }
}
