package datawave.microservice.querymetric;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.merge.MergingLastUpdateTime;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import datawave.microservice.querymetric.handler.QueryMetricCombiner;

public class QueryMetricSplitBrainMergePolicy<V extends QueryMetricUpdate,T extends MergingLastUpdateTime<V>> implements SplitBrainMergePolicy<V,T> {
    
    protected QueryMetricCombiner queryMetricCombiner;
    
    public QueryMetricSplitBrainMergePolicy() {
        this.queryMetricCombiner = getQueryMetricCombiner();
    }
    
    protected QueryMetricCombiner getQueryMetricCombiner() {
        return new QueryMetricCombiner();
    }
    
    @Override
    public V merge(T mergingValue, T existingValue) {
        if (existingValue == null) {
            return mergingValue.getDeserializedValue();
        } else {
            
            QueryMetricUpdate merging = mergingValue.getDeserializedValue();
            QueryMetricUpdate existing = existingValue.getDeserializedValue();
            QueryMetricUpdate metricUpdate;
            
            try {
                BaseQueryMetric metric = this.queryMetricCombiner.combineMetrics(merging.getMetric(), existing.getMetric(), existing.getMetricType());
                metricUpdate = new QueryMetricUpdate(metric, existing.getMetricType());
            } catch (Exception e) {
                if (existingValue.getLastUpdateTime() >= mergingValue.getLastUpdateTime()) {
                    metricUpdate = existing;
                } else {
                    metricUpdate = merging;
                }
            }
            return (V) metricUpdate;
        }
    }
    
    @Override
    public void readData(ObjectDataInput in) {
        
    }
    
    @Override
    public void writeData(ObjectDataOutput out) {
        
    }
}
