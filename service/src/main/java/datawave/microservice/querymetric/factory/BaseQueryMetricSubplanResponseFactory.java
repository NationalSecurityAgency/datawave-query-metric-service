package datawave.microservice.querymetric.factory;

import datawave.microservice.querymetric.BaseQueryMetricSubplanResponse;

public interface BaseQueryMetricSubplanResponseFactory<T extends BaseQueryMetricSubplanResponse> {
    
    T createSubplanResponse();
}
