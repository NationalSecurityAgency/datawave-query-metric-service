package datawave.microservice.querymetric;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import java.io.Serializable;

@XmlAccessorType(XmlAccessType.NONE)
public class QueryMetricUpdate<T extends BaseQueryMetric> implements Serializable {
    
    @XmlElement
    private T metric;
    
    @XmlElement
    private QueryMetricType metricType;
    
    /* constructor for deserializing JSON messages */
    public QueryMetricUpdate() {
        
    }
    
    public QueryMetricUpdate(T metric, QueryMetricType metricType) {
        this.metric = metric;
        this.metricType = metricType;
    }
    
    public QueryMetricUpdate(T metric) {
        this(metric, QueryMetricType.COMPLETE);
    }
    
    public void setMetric(T metric) {
        this.metric = metric;
    }
    
    public T getMetric() {
        return metric;
    }
    
    public void setMetricType(QueryMetricType metricType) {
        this.metricType = metricType;
    }
    
    public QueryMetricType getMetricType() {
        return metricType;
    }
}
