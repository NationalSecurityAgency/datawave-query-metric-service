package datawave.microservice.querymetric;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import datawave.microservice.query.QueryImpl;

public interface QueryMetricModelFormat {
    
    NumberFormat nf = NumberFormat.getIntegerInstance();
    
    void setBasePath(String basePath);
    
    String getCreateDateStr();
    
    String getBeginDateStr();
    
    String getEndDateStr();
    
    String getQueryIdUrl();
    
    String getProxyServersStr();
    
    String getParametersStr();
    
    String getQueryAuthorizationsStr();
    
    String getPredictionsStr();
    
    String getLoginTimeStr();
    
    String getSetupTimeStr();
    
    String getCreateCallTimeStr();
    
    String getNumPagesStr();
    
    String getNumResultsStr();
    
    String getDocRangesStr();
    
    String getFiRangesStr();
    
    String getSourceCountStr();
    
    String getNextCountStr();
    
    String getSeekCountStr();
    
    String getYieldCountStr();
    
    String getVersionStr();
    
    String getTotalPageTimeStr();
    
    String getTotalPageCallTimeStr();
    
    String getTotalSerializationTimeStr();
    
    String getTotalBytesSentStr();
    
    String getElapsedTimeStr();
    
    List<PageMetricModel> getPageTimeModels();
    
    default String getPredictionsStr(Set<BaseQueryMetric.Prediction> predictions) {
        StringBuilder builder = new StringBuilder();
        if (predictions != null && !predictions.isEmpty()) {
            String delimiter = "";
            List<BaseQueryMetric.Prediction> predictionsList = new ArrayList<>(predictions);
            Collections.sort(predictionsList);
            for (BaseQueryMetric.Prediction p : predictionsList) {
                builder.append(delimiter).append(p.getName()).append(" = ").append(p.getPrediction());
                delimiter = "<br>";
            }
        } else {
            builder.append("");
        }
        return builder.toString();
    }
    
    default boolean isJexlQuery(Set<QueryImpl.Parameter> params) {
        return params.stream().anyMatch(p -> p.getParameterName().equals("query.syntax") && p.getParameterValue().equals("JEXL"));
    }
    
    default String numToString(long number, long minValue) {
        return number < minValue ? "" : nf.format(number);
    }
    
    default String toFormattedParametersString(final Set<QueryImpl.Parameter> parameters) {
        final StringBuilder params = new StringBuilder();
        final String PARAMETER_SEPARATOR = "<BR/>";
        final String PARAMETER_NAME_VALUE_SEPARATOR = ":";
        
        if (null != parameters) {
            for (final QueryImpl.Parameter param : parameters) {
                if (params.length() > 0) {
                    params.append(PARAMETER_SEPARATOR);
                }
                
                params.append(param.getParameterName());
                params.append(PARAMETER_NAME_VALUE_SEPARATOR);
                params.append(param.getParameterValue());
            }
        }
        return params.toString();
    }
}
