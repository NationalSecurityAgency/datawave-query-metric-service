package datawave.microservice.querymetric.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.SubscribableChannel;

@Configuration
@EnableBinding({QueryMetricSinkConfiguration.QueryMetricSinkBinding.class})
@ConditionalOnProperty(name = "datawave.query.metric.client.sink.enabled", havingValue = "true", matchIfMissing = true)
public class QueryMetricSinkConfiguration {
    
    public interface QueryMetricSinkBinding {
        String SINK_NAME = "queryMetricSink";
        
        @Input(SINK_NAME)
        SubscribableChannel queryMetricSink();
    }
}
