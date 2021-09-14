package datawave.microservice.querymetric.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.SubscribableChannel;

@Configuration
@EnableBinding({QueryMetricSourceConfiguration.QueryMetricSourceBinding.class})
@ConditionalOnProperty(name = "datawave.query.metric.client.source.enabled", havingValue = "true", matchIfMissing = true)
public class QueryMetricSourceConfiguration {
    
    public interface QueryMetricSourceBinding {
        String SOURCE_NAME = "queryMetricSource";
        
        @Output(SOURCE_NAME)
        SubscribableChannel queryMetricSource();
    }
}
