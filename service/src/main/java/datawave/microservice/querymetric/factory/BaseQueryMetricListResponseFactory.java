package datawave.microservice.querymetric.factory;

import datawave.microservice.querymetric.BaseQueryMetricListResponse;

public interface BaseQueryMetricListResponseFactory<T extends BaseQueryMetricListResponse> {
    
    T createResponse();
    
    T createDetailedResponse();
}
