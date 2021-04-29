package datawave.microservice.querymetric;

public class QueryMetricFactoryImpl implements QueryMetricFactory {
    
    @Override
    public BaseQueryMetric createMetric() {
        return new QueryMetric();
    }
    
}
