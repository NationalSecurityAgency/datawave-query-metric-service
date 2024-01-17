package datawave.microservice.querymetric;

import java.util.HashMap;
import java.util.Map;

import datawave.microservice.querymetric.BaseQueryMetric.Lifecycle;

public class QueryMetricUpdateHolder<T extends BaseQueryMetric> extends QueryMetricUpdate<T> {
    
    private boolean persisted = false;
    private Lifecycle lowestLifecycle;
    private Map<String,Long> values = new HashMap<>();
    
    public QueryMetricUpdateHolder(T metric, QueryMetricType metricType) {
        super(metric, metricType);
        this.lowestLifecycle = this.metric.getLifecycle();
    }
    
    public QueryMetricUpdateHolder(T metric) {
        this(metric, QueryMetricType.COMPLETE);
    }
    
    public QueryMetricUpdateHolder(QueryMetricUpdate metricUpdate) {
        this((T) metricUpdate.getMetric(), metricUpdate.getMetricType());
    }
    
    // If we know that this metric has been persisted by the AccumuloMapStore, then it is not new
    // Because the metric can be ejected from the incoming cache, we also track the lowest lifecycle
    public boolean isNewMetric() {
        return !persisted && (lowestLifecycle == null || lowestLifecycle.equals(Lifecycle.DEFINED));
    }
    
    public void addValue(String key, Long value) {
        if (values.containsKey(key)) {
            values.put(key, values.get(key) + value);
        } else {
            values.put(key, value);
        }
    }
    
    public Long getValue(String key) {
        if (values.containsKey(key)) {
            return values.get(key);
        } else {
            return 0l;
        }
    }
    
    public void setPersisted() {
        persisted = true;
        values.clear();
        lowestLifecycle = null;
    }
    
    public Lifecycle getLowestLifecycle() {
        return lowestLifecycle;
    }
    
    public void updateLowestLifecycle(Lifecycle lifecycle) {
        if (!persisted && lifecycle != null) {
            if (this.lowestLifecycle == null || (lifecycle.ordinal() < this.lowestLifecycle.ordinal())) {
                this.lowestLifecycle = lifecycle;
            }
        }
    }
    
    @Override
    public void setMetric(T metric) {
        super.setMetric(metric);
        if (this.lowestLifecycle == null || this.metric.getLifecycle().ordinal() < this.lowestLifecycle.ordinal()) {
            this.lowestLifecycle = this.metric.getLifecycle();
        }
    }
}
