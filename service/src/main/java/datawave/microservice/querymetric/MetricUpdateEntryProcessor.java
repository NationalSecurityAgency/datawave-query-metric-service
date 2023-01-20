package datawave.microservice.querymetric;

import com.hazelcast.map.EntryProcessor;
import datawave.microservice.querymetric.handler.QueryMetricCombiner;
import java.util.Map;

public class MetricUpdateEntryProcessor implements EntryProcessor<String,QueryMetricUpdateHolder,Long> {
    
    private QueryMetricCombiner combiner;
    private QueryMetricUpdateHolder metricUpdate;
    
    public MetricUpdateEntryProcessor(QueryMetricUpdateHolder metricUpdate, QueryMetricCombiner combiner) {
        this.metricUpdate = metricUpdate;
        this.combiner = combiner;
    }
    
    @Override
    public Long process(Map.Entry<String,QueryMetricUpdateHolder> entry) {
        long start = System.currentTimeMillis();
        BaseQueryMetric combinedMetric;
        if (entry.getValue() == null) {
            entry.setValue(this.metricUpdate);
        } else {
            QueryMetricType metricType = this.metricUpdate.getMetricType();
            BaseQueryMetric storedMetric = entry.getValue().getMetric();
            BaseQueryMetric updatedMetric = this.metricUpdate.getMetric();
            combinedMetric = this.combiner.combineMetrics(updatedMetric, storedMetric, metricType);
            combinedMetric.setNumUpdates(storedMetric.getNumUpdates() + updatedMetric.getNumUpdates());
            boolean isNewMetric = entry.getValue().isNewMetric();
            if (isNewMetric == false) {
                isNewMetric = QueryMetricUpdateHolder.isNewMetric(storedMetric) || QueryMetricUpdateHolder.isNewMetric(updatedMetric);
            }
            entry.setValue(new QueryMetricUpdateHolder(combinedMetric, metricType, isNewMetric));
        }
        return Long.valueOf(System.currentTimeMillis() - start);
    }
}
