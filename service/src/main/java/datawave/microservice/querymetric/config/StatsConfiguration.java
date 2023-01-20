package datawave.microservice.querymetric.config;

import datawave.microservice.querymetric.QueryMetricOperationsStats;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.persistence.AccumuloMapStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StatsConfiguration {
    
    @Bean
    @ConditionalOnMissingBean
    QueryMetricOperationsStats queryMetricOperationsStats(TimelyProperties timelyProperties, ShardTableQueryMetricHandler handler, AccumuloMapStore mapStore) {
        return new QueryMetricOperationsStats(timelyProperties, handler, mapStore);
    }
}
