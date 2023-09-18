package datawave.microservice.querymetric;

import com.hazelcast.map.AbstractEntryProcessor;
import datawave.microservice.querymetric.handler.QueryMetricCombiner;

import java.util.Map;

public class MetricUpdateEntryProcessor extends AbstractEntryProcessor<String,QueryMetricUpdateHolder> {
    
    private QueryMetricCombiner combiner;
    private QueryMetricUpdateHolder metricUpdate;
    
    public MetricUpdateEntryProcessor(QueryMetricUpdateHolder metricUpdate, QueryMetricCombiner combiner) {
        this.metricUpdate = metricUpdate;
        this.combiner = combiner;
    }
    
    @Override
    public Long process(Map.Entry<String,QueryMetricUpdateHolder> entry) {
        QueryMetricUpdateHolder updatedHolder;
        QueryMetricType metricType = this.metricUpdate.getMetricType();
        BaseQueryMetric updatedMetric = this.metricUpdate.getMetric();
        long start = System.currentTimeMillis();
        if (entry.getValue() == null) {
            updatedHolder = this.metricUpdate;
        } else {
            updatedHolder = entry.getValue();
            BaseQueryMetric storedMetric = entry.getValue().getMetric();
            BaseQueryMetric combinedMetric;
            combinedMetric = this.combiner.combineMetrics(updatedMetric, storedMetric, metricType);
            updatedHolder.setMetric(combinedMetric);
            updatedHolder.setMetricType(metricType);
        }
        
        if (metricType.equals(QueryMetricType.DISTRIBUTED) && updatedMetric != null) {
            // these values are added incrementally in a distributed update. Because we can not be sure
            // exactly when the incomingQueryMetricCache value is stored, it would otherwise be possible
            // for updates to be included twice. These values are reset after being used in the AccumuloMapStore
            updatedHolder.addValue("sourceCount", updatedMetric.getSourceCount());
            updatedHolder.addValue("nextCount", updatedMetric.getNextCount());
            updatedHolder.addValue("seekCount", updatedMetric.getSeekCount());
            updatedHolder.addValue("yieldCount", updatedMetric.getYieldCount());
            updatedHolder.addValue("docRanges", updatedMetric.getDocRanges());
            updatedHolder.addValue("fiRanges", updatedMetric.getFiRanges());
        }
        entry.setValue(updatedHolder);
        return Long.valueOf(System.currentTimeMillis() - start);
    }
}
