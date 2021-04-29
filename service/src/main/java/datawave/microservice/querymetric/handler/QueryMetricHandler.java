package datawave.microservice.querymetric.handler;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.webservice.query.Query;

import java.util.Map;

public interface QueryMetricHandler<T extends BaseQueryMetric> {
    
    T combineMetrics(T metric, T cachedQueryMetric) throws Exception;
    
    Map<String,String> getEventFields(BaseQueryMetric queryMetric);
    
    ContentQueryMetricsIngestHelper getQueryMetricsIngestHelper(boolean deleteMode);
    
    Query createQuery();
    
    void flush() throws Exception;
    
    /**
     * Tells this handler to reload any dependent resources. This method might be called in the event of a failed write or flush to re-open any connections to
     * external resources such as Accumulo.
     */
    void reload();
}
