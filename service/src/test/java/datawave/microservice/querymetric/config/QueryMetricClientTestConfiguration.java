package datawave.microservice.querymetric.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.QueryMetricTestClient;
import datawave.security.authorization.JWTTokenHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@ConditionalOnProperty(name = "datawave.query.metric.client.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties({QueryMetricClientProperties.class})
@EnableBinding({QueryMetricSinkConfiguration.QueryMetricSinkBinding.class, QueryMetricSourceConfiguration.QueryMetricSourceBinding.class})
public class QueryMetricClientTestConfiguration {
    
    @Bean
    @Primary
    QueryMetricClient queryMetricTestClient(RestTemplateBuilder restTemplateBuilder, QueryMetricClientProperties queryMetricClientProperties,
                    QueryMetricSourceConfiguration.QueryMetricSourceBinding queryMetricSourceBinding,
                    QueryMetricSinkConfiguration.QueryMetricSinkBinding queryMetricSinkBinding, ObjectMapper objectMapper,
                    @Autowired(required = false) JWTTokenHandler jwtTokenHandler) {
        return new QueryMetricTestClient(restTemplateBuilder, queryMetricClientProperties, queryMetricSourceBinding, queryMetricSinkBinding, objectMapper,
                        jwtTokenHandler);
    }
}
