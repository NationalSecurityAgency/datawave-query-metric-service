package datawave.microservice.querymetric.alternate;

import com.google.common.collect.Multimap;
import datawave.microservice.querymetric.handler.ContentQueryMetricsIngestHelper;

public class AlternateContentQueryMetricsIngestHelper extends ContentQueryMetricsIngestHelper {
    
    public AlternateContentQueryMetricsIngestHelper(boolean deleteMode) {
        super(deleteMode, new HelperDelegate());
    }
    
    private static class HelperDelegate<T extends AlternateQueryMetric> extends ContentQueryMetricsIngestHelper.HelperDelegate<AlternateQueryMetric> {
        @Override
        protected void putExtendedFieldsToWrite(AlternateQueryMetric updatedQueryMetric, Multimap<String,String> fields) {
            if (updatedQueryMetric.getExtraField() != null) {
                fields.put("EXTRA_FIELD", updatedQueryMetric.getExtraField());
            }
        }
    }
}
