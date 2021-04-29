package datawave.microservice.querymetric.config;

import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.handler.ContentQueryMetricsIngestHelper;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.webservice.common.connection.AccumuloConnectionPool;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;

public class AlternateShardTableQueryMetricHandler extends ShardTableQueryMetricHandler<AlternateQueryMetric> {
    
    public AlternateShardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties,
                    @Qualifier("warehouse") AccumuloConnectionPool connectionPool, QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory,
                    MarkingFunctions markingFunctions) {
        super(queryMetricHandlerProperties, connectionPool, logicFactory, metricFactory, new datawave.webservice.query.cache.QueryMetricFactoryImpl(),
                        markingFunctions);
    }
    
    @Override
    public ContentQueryMetricsIngestHelper getQueryMetricsIngestHelper(boolean deleteMode) {
        return new AlternateContentQueryMetricsIngestHelper(deleteMode);
    }
    
    @Override
    public AlternateQueryMetric toMetric(EventBase event) {
        AlternateQueryMetric queryMetric = super.toMetric(event);
        List<FieldBase> fields = event.getFields();
        fields.forEach(f -> {
            if (f.getName().equals("EXTRA_FIELD")) {
                queryMetric.setExtraField(f.getValueString());
            }
        });
        return queryMetric;
    }
}
