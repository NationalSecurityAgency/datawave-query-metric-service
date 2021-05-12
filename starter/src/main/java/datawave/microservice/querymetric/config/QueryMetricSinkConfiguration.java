package datawave.microservice.querymetric.config;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.SubscribableChannel;

@Configuration
public class QueryMetricSinkConfiguration {
    
    public interface QueryMetricSinkBinding {
        String SINK_NAME = "queryMetricSink";
        
        @Input(SINK_NAME)
        SubscribableChannel queryMetricSink();
    }
}
