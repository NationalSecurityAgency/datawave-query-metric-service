package datawave.microservice.querymetric;

import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.querymetric.config.QueryMetricClientProperties;
import datawave.microservice.querymetric.config.QueryMetricTransportType;
import datawave.microservice.querymetric.function.QueryMetricConsumer;
import datawave.microservice.querymetric.function.QueryMetricSupplier;
import datawave.security.authorization.JWTTokenHandler;
import org.springframework.boot.web.client.RestTemplateBuilder;

/* Rest and spring cloud stream client for submitting query metric updates to the query metric service
 *
 * @see Request
*/
public class QueryMetricTestClient extends QueryMetricClient {
    
    private QueryMetricConsumer queryMetricConsumer;
    private QueryMetricClientProperties queryMetricClientProperties;
    
    public QueryMetricTestClient(RestTemplateBuilder restTemplateBuilder, QueryMetricClientProperties queryMetricClientProperties,
                    QueryMetricSupplier queryMetricSupplier, QueryMetricConsumer queryMetricConsumer, ObjectMapper objectMapper,
                    JWTTokenHandler jwtTokenHandler) {
        super(restTemplateBuilder, queryMetricClientProperties, queryMetricSupplier, objectMapper, jwtTokenHandler);
        this.queryMetricConsumer = queryMetricConsumer;
        this.queryMetricClientProperties = queryMetricClientProperties;
    }
    
    public void submit(Request request) throws Exception {
        if (request.metrics == null || request.metrics.isEmpty()) {
            throw new IllegalArgumentException("Request must contain a query metric");
        }
        if (request.metricType == null) {
            throw new IllegalArgumentException("Request must contain a query metric type");
        }
        
        if (queryMetricClientProperties.getTransport().equals(QueryMetricTransportType.HTTP)
                        || queryMetricClientProperties.getTransport().equals(QueryMetricTransportType.HTTPS)) {
            super.submit(request);
        } else {
            for (BaseQueryMetric metric : request.metrics) {
                QueryMetricUpdate metricUpdate = new QueryMetricUpdate(metric, request.metricType);
                queryMetricConsumer.accept(metricUpdate);
            }
        }
    }
}
