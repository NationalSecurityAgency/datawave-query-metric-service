package datawave.microservice.querymetric;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.merge.MergingLastUpdateTime;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import datawave.microservice.querymetric.handler.QueryMetricCombiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryMetricSplitBrainMergePolicy<V extends QueryMetricUpdateHolder,T extends MergingLastUpdateTime<V>> implements SplitBrainMergePolicy<V,T> {
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    protected QueryMetricCombiner queryMetricCombiner;
    
    public QueryMetricSplitBrainMergePolicy() {
        this.queryMetricCombiner = getQueryMetricCombiner();
    }
    
    protected QueryMetricCombiner getQueryMetricCombiner() {
        return new QueryMetricCombiner();
    }
    
    @Override
    public V merge(T mergingValue, T existingValue) {
        QueryMetricUpdateHolder mergedValue;
        if (existingValue == null) {
            mergedValue = mergingValue.getDeserializedValue();
            if (log.isTraceEnabled()) {
                log.trace("Merged metrics existing is null, using merging: " + mergedValue.getMetric());
            } else {
                log.debug("Merged metric: " + mergedValue.getMetric().getQueryId());
            }
        } else {
            
            QueryMetricUpdateHolder merging = mergingValue.getDeserializedValue();
            QueryMetricUpdateHolder existing = existingValue.getDeserializedValue();
            
            try {
                BaseQueryMetric metric = this.queryMetricCombiner.combineMetrics(merging.getMetric(), existing.getMetric(), existing.getMetricType());
                mergedValue = new QueryMetricUpdateHolder(metric, existing.getMetricType());
            } catch (Exception e) {
                if (existingValue.getLastUpdateTime() >= mergingValue.getLastUpdateTime()) {
                    mergedValue = existing;
                } else {
                    mergedValue = merging;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("Merged metrics existing: " + existing.getMetric() + " merging: " + merging.getMetric() + " merged: " + mergedValue.getMetric());
            } else {
                log.debug("Merged metric: " + mergedValue.getMetric().getQueryId());
            }
        }
        
        return (V) mergedValue;
    }
    
    @Override
    public void readData(ObjectDataInput in) {
        
    }
    
    @Override
    public void writeData(ObjectDataOutput out) {
        
    }
}
