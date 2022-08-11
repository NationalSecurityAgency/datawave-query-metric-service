package datawave.microservice.querymetric;

import datawave.webservice.query.QueryImpl.Parameter;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Set;

@XmlRootElement(name = "QueryMetricListResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryMetricsDetailListResponse extends QueryMetricListResponse {
    
    private static final long serialVersionUID = 1L;
    
    static boolean isJexlQuery(Set<Parameter> params) {
        return params.stream().anyMatch(p -> p.getParameterName().equals("query.syntax") && p.getParameterValue().equals("JEXL"));
    }
    
    static String numToString(long number, long minValue) {
        return number < minValue ? "" : Long.toString(number);
    }
    
    static String toFormattedParametersString(final Set<Parameter> parameters) {
        final StringBuilder params = new StringBuilder();
        final String PARAMETER_SEPARATOR = ";";
        final String PARAMETER_NAME_VALUE_SEPARATOR = ":";
        final String NEWLINE = System.getProperty("line.separator");
        
        if (null != parameters) {
            for (final Parameter param : parameters) {
                if (params.length() > 0) {
                    params.append(PARAMETER_SEPARATOR + NEWLINE);
                }
                
                params.append(param.getParameterName());
                params.append(PARAMETER_NAME_VALUE_SEPARATOR);
                params.append(param.getParameterValue());
            }
        }
        
        return params.toString();
    }
}
