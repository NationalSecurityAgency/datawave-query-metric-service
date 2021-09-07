package datawave.microservice.querymetric.alternate;

import datawave.microservice.querymetric.QueryMetricUpdate;
import datawave.microservice.querymetric.function.QueryMetricSupplier;
import org.springframework.messaging.Message;

// extending QueryMetricSupplier to add count/reset functionality for testing
public class AlternateQueryMetricSupplier extends QueryMetricSupplier {
    
    public long messagesSent = 0;
    
    @Override
    public boolean send(Message<QueryMetricUpdate> queryMetricUpdate) {
        messagesSent++;
        return super.send(queryMetricUpdate);
    }
    
    public long getMessagesSent() {
        return messagesSent;
    }
    
    public void reset() {
        this.messagesSent = 0;
    }
}
