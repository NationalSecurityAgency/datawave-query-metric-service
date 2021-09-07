package datawave.microservice.querymetric.alternate;

import datawave.microservice.querymetric.QueryMetric;

import javax.xml.bind.annotation.XmlElement;

public class AlternateQueryMetric extends QueryMetric {
    
    @XmlElement
    private String extraField;
    
    public AlternateQueryMetric() {
        super();
    }
    
    public void setExtraField(String extraField) {
        this.extraField = extraField;
    }
    
    public String getExtraField() {
        return extraField;
    }
}
