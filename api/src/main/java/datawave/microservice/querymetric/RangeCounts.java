package datawave.microservice.querymetric;

public class RangeCounts {
    
    private long documentRangeCount;
    private long shardRangeCount;
    
    public long getDocumentRangeCount() {
        return documentRangeCount;
    }
    
    public long getShardRangeCount() {
        return shardRangeCount;
    }
    
    public void setDocumentRangeCount(long newDocumentRangeCount) {
        this.documentRangeCount = newDocumentRangeCount;
    }
    
    public void setShardRangeCount(long newShardRangeCount) {
        this.shardRangeCount = newShardRangeCount;
    }
}
