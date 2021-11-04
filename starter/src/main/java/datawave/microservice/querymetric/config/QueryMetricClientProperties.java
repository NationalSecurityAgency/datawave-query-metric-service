package datawave.microservice.querymetric.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datawave.query.metric.client")
public class QueryMetricClientProperties {
    private boolean enabled;
    private QueryMetricTransportType transport = QueryMetricTransportType.MESSAGE;
    private String host = "localhost";
    private int port = 8443;
    private String updateMetricUrl = "/querymetric/v1/updateMetric";
    private String updateMetricsUrl = "/querymetric/v1/updateMetrics";
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setTransport(QueryMetricTransportType transport) {
        this.transport = transport;
    }
    
    public QueryMetricTransportType getTransport() {
        return transport;
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
    public void setPort(int port) {
        this.port = port;
    }
    
    public String getUpdateMetricUrl() {
        return updateMetricUrl;
    }
    
    public void setUpdateMetricUrl(String updateMetricUrl) {
        this.updateMetricUrl = updateMetricUrl;
    }
    
    public String getUpdateMetricsUrl() {
        return updateMetricsUrl;
    }
    
    public void setUpdateMetricsUrl(String updateMetricsUrl) {
        this.updateMetricsUrl = updateMetricsUrl;
    }
}
