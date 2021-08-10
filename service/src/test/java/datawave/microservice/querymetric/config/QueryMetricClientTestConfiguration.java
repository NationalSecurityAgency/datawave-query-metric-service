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

@Configuration
@EnableConfigurationProperties({QueryMetricClientProperties.class})
@EnableBinding({QueryMetricSinkConfiguration.QueryMetricSinkBinding.class, QueryMetricSourceConfiguration.QueryMetricSourceBinding.class})
public class QueryMetricClientTestConfiguration {
    
    // A JWTTokenHandler is only necessary for HTTP and HTTPS transportTypes
    // and should not cause an autowire failure if it is not present
    @Autowired(required = false)
    private JWTTokenHandler jwtTokenHandler;
    
    @Bean
    @ConditionalOnProperty(name = "datawave.query.metric.client.enabled", havingValue = "true", matchIfMissing = true)
    QueryMetricClient queryMetricClient(RestTemplateBuilder restTemplateBuilder, QueryMetricClientProperties queryMetricClientProperties,
                    QueryMetricSourceConfiguration.QueryMetricSourceBinding queryMetricSourceBinding,
                    QueryMetricSinkConfiguration.QueryMetricSinkBinding queryMetricSinkBinding, ObjectMapper objectMapper) {
        QueryMetricClient queryMetricClient = new QueryMetricTestClient(restTemplateBuilder, queryMetricClientProperties, queryMetricSourceBinding,
                        queryMetricSinkBinding, objectMapper);
        if (this.jwtTokenHandler != null) {
            queryMetricClient.setJwtTokenHandler(this.jwtTokenHandler);
        }
        return queryMetricClient;
    }
}
