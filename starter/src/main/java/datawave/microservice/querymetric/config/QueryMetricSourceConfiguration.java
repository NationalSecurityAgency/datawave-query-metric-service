package datawave.microservice.querymetric.config;

import datawave.microservice.querymetric.function.QueryMetricSupplier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "datawave.query.metric.client.source.enabled", havingValue = "true", matchIfMissing = true)
public class QueryMetricSourceConfiguration {
    @Bean
    public QueryMetricSupplier queryMetricSource() {
        return new QueryMetricSupplier();
    }
}
