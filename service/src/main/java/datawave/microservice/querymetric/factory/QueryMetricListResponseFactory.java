package datawave.microservice.querymetric.factory;

import datawave.microservice.querymetric.BaseQueryMetricListResponse;
import datawave.microservice.querymetric.QueryMetricListResponse;

public class QueryMetricListResponseFactory implements BaseQueryMetricListResponseFactory {
    
    @Override
    public BaseQueryMetricListResponse createResponse() {
        return new QueryMetricListResponse();
    }
}
