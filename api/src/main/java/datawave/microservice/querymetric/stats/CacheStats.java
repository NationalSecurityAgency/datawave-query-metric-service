package datawave.microservice.querymetric.stats;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@XmlRootElement(name = "CacheStats")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class CacheStats implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @XmlAttribute
    private String host;
    
    @XmlAttribute
    private String memberUuid;
    
    @XmlElement(name = "incomingQueryMetrics")
    @XmlJavaTypeAdapter(LongMapAdapter.class)
    private Map<String,Long> incomingQueryMetrics = new HashMap<>();
    
    @XmlElement(name = "lastWrittenQueryMetrics")
    @XmlJavaTypeAdapter(LongMapAdapter.class)
    private Map<String,Long> lastWrittenQueryMetrics = new HashMap<>();
    
    public CacheStats() {
        
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public String getHost() {
        return host;
    }
    
    public void setMemberUuid(String memberUuid) {
        this.memberUuid = memberUuid;
    }
    
    public String getMemberUuid() {
        return memberUuid;
    }
    
    public void setIncomingQueryMetrics(Map<String,Long> stats) {
        this.incomingQueryMetrics = stats;
    }
    
    public Map<String,Long> getIncomingQueryMetrics() {
        return incomingQueryMetrics;
    }
    
    public void setLastWrittenQueryMetrics(Map<String,Long> stats) {
        this.lastWrittenQueryMetrics = stats;
    }
    
    public Map<String,Long> getLastWrittenQueryMetrics() {
        return lastWrittenQueryMetrics;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        CacheStats that = (CacheStats) o;
        return incomingQueryMetrics.equals(that.incomingQueryMetrics) && lastWrittenQueryMetrics.equals(that.lastWrittenQueryMetrics);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(incomingQueryMetrics, lastWrittenQueryMetrics);
    }
}
