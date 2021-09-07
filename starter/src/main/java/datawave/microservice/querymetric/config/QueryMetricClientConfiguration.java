package datawave.microservice.querymetric.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.function.QueryMetricSupplier;
import datawave.security.authorization.JWTTokenHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({QueryMetricClientProperties.class})
public class QueryMetricClientConfiguration {
    
    @Bean
    @ConditionalOnProperty(name = "datawave.query.metric.client.enabled", havingValue = "true", matchIfMissing = true)
    QueryMetricClient queryMetricClient(RestTemplateBuilder restTemplateBuilder, QueryMetricClientProperties queryMetricClientProperties,
                    @Autowired(required = false) QueryMetricSupplier queryMetricSupplier, ObjectMapper objectMapper,
                    @Autowired(required = false) JWTTokenHandler jwtTokenHandler) {
        return new QueryMetricClient(restTemplateBuilder, queryMetricClientProperties, queryMetricSupplier, objectMapper, jwtTokenHandler);
    }
}
