package datawave.microservice.querymetric.factory;

import datawave.microservice.querymetric.BaseQueryMetricSubplanResponse;
import datawave.microservice.querymetric.QueryMetricsSubplanResponse;

public class QueryMetricSubplanResponseFactory implements BaseQueryMetricSubplanResponseFactory {
    
    @Override
    public BaseQueryMetricSubplanResponse createSubplanResponse() {
        return new QueryMetricsSubplanResponse();
    }
}
