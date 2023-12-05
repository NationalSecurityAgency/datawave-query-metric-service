package datawave.microservice.querymetric.config;

import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.microservice.querymetric.handler.ContentQueryMetricsIngestHelper;
import datawave.microservice.querymetric.handler.QueryMetricCombiner;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.webservice.common.connection.AccumuloClientPool;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Collection;
import java.util.List;

public class AlternateShardTableQueryMetricHandler extends ShardTableQueryMetricHandler<AlternateQueryMetric> {
    
    public AlternateShardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties,
                    @Qualifier("warehouse") AccumuloClientPool accumuloClientPool, QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory,
                    MarkingFunctions markingFunctions, QueryMetricCombiner queryMetricCombiner, LuceneToJexlQueryParser luceneToJexlQueryParser) {
        super(queryMetricHandlerProperties, accumuloClientPool, logicFactory, metricFactory, markingFunctions, queryMetricCombiner, luceneToJexlQueryParser);
    }
    
    @Override
    public ContentQueryMetricsIngestHelper getQueryMetricsIngestHelper(boolean deleteMode, Collection<String> ignoredFields) {
        return new AlternateContentQueryMetricsIngestHelper(deleteMode, ignoredFields);
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
