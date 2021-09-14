package datawave.microservice.querymetric.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.config.QueryMetricSinkConfiguration.QueryMetricSinkBinding;
import datawave.microservice.querymetric.config.QueryMetricSourceConfiguration.QueryMetricSourceBinding;
import datawave.security.authorization.JWTTokenHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "datawave.query.metric.client.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties({QueryMetricClientProperties.class})
@EnableBinding({QueryMetricSinkBinding.class, QueryMetricSourceBinding.class})
public class QueryMetricClientConfiguration {
    
    @Bean
    QueryMetricClient queryMetricClient(RestTemplateBuilder restTemplateBuilder, QueryMetricClientProperties queryMetricClientProperties,
                    @Autowired(required = false) QueryMetricSourceBinding queryMetricSourceBinding, ObjectMapper objectMapper,
                    @Autowired(required = false) JWTTokenHandler jwtTokenHandler) {
        return new QueryMetricClient(restTemplateBuilder, queryMetricClientProperties, queryMetricSourceBinding, objectMapper, jwtTokenHandler);
    }
}
