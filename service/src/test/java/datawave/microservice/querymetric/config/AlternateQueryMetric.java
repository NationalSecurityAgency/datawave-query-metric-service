package datawave.microservice.querymetric.config;

import datawave.webservice.query.metric.QueryMetric;

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
