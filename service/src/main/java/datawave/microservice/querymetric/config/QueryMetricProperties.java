package datawave.microservice.querymetric.config;

import java.util.concurrent.TimeUnit;

import javax.validation.Valid;
import javax.validation.constraints.PositiveOrZero;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "datawave.query.metric")
public class QueryMetricProperties {
    
    @Valid
    private Retry retry = new Retry();
    
    public Retry getRetry() {
        return retry;
    }
    
    public void setRetry(Retry retry) {
        this.retry = retry;
    }
    
    @Validated
    public static class Retry {
        @PositiveOrZero
        private int maxAttempts = 10;
        
        @PositiveOrZero
        private long failTimeoutMillis = TimeUnit.MINUTES.toMillis(5);
        
        @PositiveOrZero
        private long backoffIntervalMillis = TimeUnit.SECONDS.toMillis(5);
        
        public int getMaxAttempts() {
            return maxAttempts;
        }
        
        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }
        
        public long getFailTimeoutMillis() {
            return failTimeoutMillis;
        }
        
        public void setFailTimeoutMillis(long failTimeoutMillis) {
            this.failTimeoutMillis = failTimeoutMillis;
        }
        
        public long getBackoffIntervalMillis() {
            return backoffIntervalMillis;
        }
        
        public void setBackoffIntervalMillis(long backoffIntervalMillis) {
            this.backoffIntervalMillis = backoffIntervalMillis;
        }
    }
}
