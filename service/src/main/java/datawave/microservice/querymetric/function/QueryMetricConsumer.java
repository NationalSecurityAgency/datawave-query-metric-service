package datawave.microservice.querymetric.function;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.microservice.querymetric.QueryMetricOperations;
import datawave.microservice.querymetric.QueryMetricUpdate;

public class QueryMetricConsumer implements Consumer<QueryMetricUpdate> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private QueryMetricOperations queryMetricOperations;
    
    public QueryMetricConsumer(QueryMetricOperations queryMetricOperations) {
        this.queryMetricOperations = queryMetricOperations;
    }
    
    @Override
    public void accept(QueryMetricUpdate queryMetricUpdate) {
        try {
            queryMetricOperations.storeMetric(queryMetricUpdate);
        } catch (Exception e) {
            log.error("Error processing query metric update message: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
