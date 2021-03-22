package datawave.microservice.querymetric.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@ConfigurationProperties(prefix = "datawave.metadata")
public class MetadataProperties {
    
    private Set<String> allMetadataAuths = Collections.emptySet();
    private Map<String,String> typeSubstitutions = Collections.emptyMap();
    
    public void setAllMetadataAuths(Set<String> allMetadataAuths) {
        this.allMetadataAuths = allMetadataAuths;
    }
    
    public Set<String> getAllMetadataAuths() {
        return allMetadataAuths;
    }
    
    public void setTypeSubstitutions(Map<String,String> typeSubstitutions) {
        this.typeSubstitutions = typeSubstitutions;
    }
    
    public Map<String,String> getTypeSubstitutions() {
        return typeSubstitutions;
    }
}
