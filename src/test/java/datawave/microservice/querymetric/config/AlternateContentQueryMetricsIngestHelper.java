package datawave.microservice.querymetric.config;

import com.google.common.collect.Multimap;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.microservice.querymetric.handler.ContentQueryMetricsIngestHelper;
import datawave.webservice.query.metric.BaseQueryMetric;

public class AlternateContentQueryMetricsIngestHelper extends ContentQueryMetricsIngestHelper {
    
    private HelperDelegate delegate = new HelperDelegate();
    
    public AlternateContentQueryMetricsIngestHelper(boolean deleteMode) {
        super(deleteMode);
    }
    
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFieldsToWrite(BaseQueryMetric updatedQueryMetric) {
        return normalize(delegate.getEventFieldsToWrite((AlternateQueryMetric) updatedQueryMetric));
    }
    
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFieldsToDelete(BaseQueryMetric updatedQueryMetric, BaseQueryMetric storedQueryMetric) {
        return normalize(delegate.getEventFieldsToDelete((AlternateQueryMetric) updatedQueryMetric, (AlternateQueryMetric) storedQueryMetric));
    }
    
    private static class HelperDelegate extends ContentQueryMetricsIngestHelper.HelperDelegate<AlternateQueryMetric> {
        
        @Override
        protected void putExtendedFieldsToWrite(AlternateQueryMetric updatedQueryMetric, Multimap<String,String> fields) {
            if (updatedQueryMetric.getExtraField() != null) {
                fields.put("EXTRA_FIELD", updatedQueryMetric.getExtraField());
            }
        }
    }
}
