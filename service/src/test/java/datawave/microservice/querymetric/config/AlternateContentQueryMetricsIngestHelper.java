package datawave.microservice.querymetric.config;

import com.google.common.collect.Multimap;
import datawave.microservice.querymetric.handler.ContentQueryMetricsIngestHelper;

public class AlternateContentQueryMetricsIngestHelper extends ContentQueryMetricsIngestHelper {
    
    public AlternateContentQueryMetricsIngestHelper(boolean deleteMode) {
        super(deleteMode, new HelperDelegate());
    }
    
    private static class HelperDelegate<T extends AlternateQueryMetric> extends ContentQueryMetricsIngestHelper.HelperDelegate<AlternateQueryMetric> {
        @Override
        protected void putExtendedFieldsToWrite(AlternateQueryMetric updated, AlternateQueryMetric stored, Multimap<String,String> fields) {
            if (isFirstWrite(updated.getExtraField(), stored == null ? null : stored.getExtraField())) {
                fields.put("EXTRA_FIELD", updated.getExtraField());
            }
        }
    }
}
