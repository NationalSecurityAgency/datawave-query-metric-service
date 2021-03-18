package datawave.microservice.querymetrics.config;

import datawave.microservice.querymetrics.handler.ContentQueryMetricsIngestHelper;
import datawave.microservice.querymetrics.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetrics.logic.QueryMetricQueryLogicFactory;
import datawave.webservice.query.cache.QueryMetricFactory;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import org.apache.accumulo.core.client.Instance;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;

public class AlternateShardTableQueryMetricHandler extends ShardTableQueryMetricHandler<AlternateQueryMetric> {
    
    public AlternateShardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties, @Qualifier("warehouse") Instance instance,
                    QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory) {
        super(queryMetricHandlerProperties, instance, logicFactory, metricFactory);
    }
    
    @Override
    public ContentQueryMetricsIngestHelper getQueryMetricsIngestHelper(boolean deleteMode) {
        return new AlternateContentQueryMetricsIngestHelper(deleteMode);
    }
    
    @Override
    public AlternateQueryMetric toMetric(EventBase event) {
        AlternateQueryMetric queryMetric = (AlternateQueryMetric) super.toMetric(event);
        List<FieldBase> fields = event.getFields();
        fields.forEach(f -> {
            if (f.getName().equals("EXTRA_FIELD")) {
                queryMetric.setExtraField(f.getValueString());
            }
        });
        return queryMetric;
    }
}
