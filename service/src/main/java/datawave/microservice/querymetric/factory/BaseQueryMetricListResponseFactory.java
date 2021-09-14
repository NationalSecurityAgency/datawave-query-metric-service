package datawave.microservice.querymetric.factory;

import datawave.microservice.querymetric.BaseQueryMetricListResponse;
import datawave.microservice.querymetric.QueryMetric;

public interface BaseQueryMetricListResponseFactory {
    
    BaseQueryMetricListResponse<QueryMetric> createResponse();
    
    BaseQueryMetricListResponse<QueryMetric> createDetailedResponse();
    
}
