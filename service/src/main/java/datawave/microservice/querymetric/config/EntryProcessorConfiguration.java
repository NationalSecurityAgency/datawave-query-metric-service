package datawave.microservice.querymetric.config;

import datawave.microservice.querymetric.MetricUpdateEntryProcessorFactory;
import datawave.microservice.querymetric.handler.QueryMetricCombiner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EntryProcessorConfiguration {
    
    @Bean
    MetricUpdateEntryProcessorFactory entryProcessorFactory(QueryMetricCombiner combiner) {
        return new MetricUpdateEntryProcessorFactory(combiner);
    }
}
