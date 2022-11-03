package datawave.microservice.querymetric;

public class QueryMetricUpdateHolder<T extends BaseQueryMetric> extends QueryMetricUpdate<T> {
    
    private boolean newMetric;
    
    public QueryMetricUpdateHolder(T metric, QueryMetricType metricType, boolean newMetric) {
        super(metric, metricType);
        this.newMetric = newMetric;
    }
    
    public QueryMetricUpdateHolder(T metric, QueryMetricType metricType) {
        this(metric, metricType, QueryMetricUpdateHolder.isNewMetric(metric));
    }
    
    public QueryMetricUpdateHolder(T metric) {
        this(metric, QueryMetricType.COMPLETE);
    }
    
    public QueryMetricUpdateHolder(QueryMetricUpdate metricUpdate) {
        this((T) metricUpdate.getMetric(), metricUpdate.getMetricType());
    }
    
    public boolean isNewMetric() {
        return newMetric;
    }
    
    public static boolean isNewMetric(BaseQueryMetric metric) {
        return metric.getLifecycle().equals(BaseQueryMetric.Lifecycle.DEFINED) || metric.getLifecycle().equals(BaseQueryMetric.Lifecycle.INITIALIZED);
    }
}
