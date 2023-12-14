package datawave.microservice.querymetric.handler;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryGeometryResponse;

import java.util.List;

public interface QueryGeometryHandler {
    
    QueryGeometryResponse getQueryGeometryResponse(String id, List<? extends BaseQueryMetric> queries);
}
