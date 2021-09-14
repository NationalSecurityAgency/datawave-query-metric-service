package datawave.microservice.querymetric;

import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.querymetric.config.QueryMetricClientProperties;
import datawave.microservice.querymetric.config.QueryMetricSinkConfiguration.QueryMetricSinkBinding;
import datawave.microservice.querymetric.config.QueryMetricSourceConfiguration.QueryMetricSourceBinding;
import datawave.microservice.querymetric.config.QueryMetricTransportType;
import datawave.security.authorization.JWTTokenHandler;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.messaging.support.MessageBuilder;

/* Rest and spring cloud stream client for submitting query metric updates to the query metric service
 *
 * @see Request
*/
public class QueryMetricTestClient extends QueryMetricClient {
    
    private QueryMetricSinkBinding queryMetricSinkBinding;
    private QueryMetricClientProperties queryMetricClientProperties;
    
    public QueryMetricTestClient(RestTemplateBuilder restTemplateBuilder, QueryMetricClientProperties queryMetricClientProperties,
                    QueryMetricSourceBinding queryMetricSourceBinding, QueryMetricSinkBinding queryMetricSinkBinding, ObjectMapper objectMapper,
                    JWTTokenHandler jwtTokenHandler) {
        super(restTemplateBuilder, queryMetricClientProperties, queryMetricSourceBinding, objectMapper, jwtTokenHandler);
        this.queryMetricSinkBinding = queryMetricSinkBinding;
        this.queryMetricClientProperties = queryMetricClientProperties;
    }
    
    public void submit(Request request) throws Exception {
        if (request.metrics == null || request.metrics.isEmpty()) {
            throw new IllegalArgumentException("Request must contain a query metric");
        }
        if (request.metricType == null) {
            throw new IllegalArgumentException("Request must contain a query metric type");
        }
        if (queryMetricClientProperties.getTransport().equals(QueryMetricTransportType.MESSAGE_TEST)) {
            submitViaMessageTest(request);
        } else {
            super.submit(request);
        }
    }
    
    private void submitViaMessageTest(Request request) {
        for (BaseQueryMetric metric : request.metrics) {
            QueryMetricUpdate metricUpdate = new QueryMetricUpdate(metric, request.metricType);
            queryMetricSinkBinding.queryMetricSink().send(MessageBuilder.withPayload(metricUpdate).build());
        }
    }
}
