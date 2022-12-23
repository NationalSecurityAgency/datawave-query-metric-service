package datawave.microservice.querymetric.handler;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.microservice.querymetric.QueryMetricsSummaryResponse;
import datawave.webservice.query.Query;

import java.util.Date;
import java.util.Map;

public interface QueryMetricHandler<T extends BaseQueryMetric> {
    
    T combineMetrics(T metric, T cachedQueryMetric, QueryMetricType metricType) throws Exception;
    
    void populateMetricSelectors(T queryMetric);
    
    Map<String,String> getEventFields(BaseQueryMetric queryMetric);
    
    ContentQueryMetricsIngestHelper getQueryMetricsIngestHelper(boolean deleteMode);
    
    Query createQuery();
    
    void flush() throws Exception;
    
    QueryMetricsSummaryResponse getQueryMetricsSummary(Date begin, Date end, DatawaveUserDetails currentUser, boolean onlyCurrentUser);
    
    /**
     * Tells this handler to reload any dependent resources. This method might be called in the event of a failed write or flush to re-open any connections to
     * external resources such as Accumulo.
     */
    void reload();
}
