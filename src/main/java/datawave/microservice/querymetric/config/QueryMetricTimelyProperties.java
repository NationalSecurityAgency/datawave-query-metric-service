package datawave.microservice.querymetric.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashSet;
import java.util.Set;

@ConfigurationProperties(prefix = "datawave.query.metric.timely")
public class QueryMetricTimelyProperties {
    
    private String timelyHost = null;
    private int timelyPort = 0;
    private Set<String> timelyMetricTags = new HashSet<>();
    
    public String getTimelyHost() {
        return timelyHost;
    }
    
    public void setTimelyHost(String timelyHost) {
        this.timelyHost = timelyHost;
    }
    
    public int getTimelyPort() {
        return timelyPort;
    }
    
    public void setTimelyPort(int timelyPort) {
        this.timelyPort = timelyPort;
    }
    
    public Set<String> getTimelyMetricTags() {
        return timelyMetricTags;
    }
    
    public void setTimelyMetricTags(Set<String> timelyMetricTags) {
        this.timelyMetricTags = timelyMetricTags;
    }
}
