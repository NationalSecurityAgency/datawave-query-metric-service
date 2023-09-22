package datawave.microservice.querymetric.config;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetric;

import javax.xml.bind.annotation.XmlElement;

public class AlternateQueryMetric extends QueryMetric {
    
    @XmlElement
    private String extraField;
    
    public AlternateQueryMetric() {
        super();
    }
    
    public AlternateQueryMetric(AlternateQueryMetric other) {
        super(other);
        this.extraField = other.extraField;
    }

    @Override
    public BaseQueryMetric duplicate() {
        return new AlternateQueryMetric(this);
    }

    public void setExtraField(String extraField) {
        this.extraField = extraField;
    }
    
    public String getExtraField() {
        return extraField;
    }
}
