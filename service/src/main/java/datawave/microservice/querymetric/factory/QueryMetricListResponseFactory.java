package datawave.microservice.querymetric.factory;

import datawave.microservice.querymetric.BaseQueryMetricListResponse;
import datawave.microservice.querymetric.QueryMetricListResponse;
import datawave.microservice.querymetric.QueryMetricsDetailListResponse;

public class QueryMetricListResponseFactory implements BaseQueryMetricListResponseFactory {
    
    @Override
    public BaseQueryMetricListResponse createResponse() {
        return new QueryMetricListResponse();
    }
    
    @Override
    public QueryMetricsDetailListResponse createDetailedResponse() {
        return new QueryMetricsDetailListResponse();
    }
}
