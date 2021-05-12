package datawave.microservice.querymetric.config;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.SubscribableChannel;

@Configuration
public class QueryMetricSourceConfiguration {
    
    public interface QueryMetricSourceBinding {
        String SOURCE_NAME = "queryMetricSource";
        
        @Output(SOURCE_NAME)
        SubscribableChannel queryMetricSource();
    }
}
