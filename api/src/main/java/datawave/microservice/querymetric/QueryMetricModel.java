package datawave.microservice.querymetric;

import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;

import datawave.webservice.query.QueryImpl;

public class QueryMetricModel extends QueryMetric {
    
    public long totalPageTime = 0;
    public long totalPageCallTime = 0;
    public long totalSerializationTime = 0;
    public long totalBytesSent = 0;
    
    private String basePath;
    private NumberFormat nf = NumberFormat.getIntegerInstance();
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
    
    public QueryMetricModel(QueryMetric queryMetric, String basePath) {
        super(queryMetric);
        this.basePath = basePath;
    }
    
    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }
    
    public String getCreateDateStr() {
        return sdf.format(createDate);
    }
    
    @Override
    public String getUser() {
        return user == null ? "" : user;
    }
    
    public String getBeginDateStr() {
        return sdf.format(beginDate);
    }
    
    public String getEndDateStr() {
        return sdf.format(endDate);
    }
    
    public String getQueryIdUrl() {
        return basePath + "/v1/id/" + queryId;
    }
    
    public String getProxyServersStr() {
        return getProxyServers() == null ? "" : StringUtils.join(getProxyServers(), "<BR/>");
    }
    
    public String getQueryStyle() {
        return isJexlQuery(parameters) ? "white-space: pre; word-wrap: break-word;" : "word-wrap: break-word;";
    }
    
    public String getParametersStr() {
        return parameters == null ? "" : toFormattedParametersString(parameters);
    }
    
    public String getQueryAuthorizationsStr() {
        return getQueryAuthorizations() == null ? "" : getQueryAuthorizations().replaceAll(",", " ");
    }
    
    public String getPredictionsStr() {
        StringBuilder builder = new StringBuilder();
        if (predictions != null && !predictions.isEmpty()) {
            String delimiter = "";
            List<Prediction> predictionsList = new ArrayList<>(predictions);
            Collections.sort(predictionsList);
            for (Prediction p : predictionsList) {
                builder.append(delimiter).append(p.getName()).append(" = ").append(p.getPrediction());
                delimiter = "<br>";
            }
        } else {
            builder.append("");
        }
        return builder.toString();
    }
    
    public String getLoginTimeStr() {
        return numToString(getLoginTime(), 0);
    }
    
    public String getSetupTimeStr() {
        return numToString(getSetupTime(), 0);
    }
    
    public String getCreateCallTimeStr() {
        return numToString(getCreateCallTime(), 0);
    }
    
    public String getNumPagesStr() {
        return nf.format(numPages);
    }
    
    public String getNumResultsStr() {
        return nf.format(numResults);
    }
    
    public String getDocRangesStr() {
        return nf.format(docRanges);
    }
    
    public String getFiRangesStr() {
        return nf.format(fiRanges);
    }
    
    public String getSourceCountStr() {
        return nf.format(sourceCount);
    }
    
    public String getNextCountStr() {
        return nf.format(nextCount);
    }
    
    public String getSeekCountStr() {
        return nf.format(seekCount);
    }
    
    public String getYieldCountStr() {
        return nf.format(yieldCount);
    }
    
    public String getVersionStr() {
        return versionMap.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("<br/>"));
    }
    
    public String getTotalPageTimeStr() {
        return nf.format(totalPageTime);
    }
    
    public String getTotalPageCallTimeStr() {
        return nf.format(totalPageCallTime);
    }
    
    public String getTotalSerializationTimeStr() {
        return nf.format(totalSerializationTime);
    }
    
    public String getTotalBytesSentStr() {
        return nf.format(totalBytesSent);
    }
    
    public String getElapsedTimeStr() {
        return nf.format(getElapsedTime());
    }
    
    @Override
    public String getErrorCode() {
        return errorCode == null ? "" : StringEscapeUtils.escapeHtml4(errorCode);
    }
    
    @Override
    public String getErrorMessage() {
        return errorMessage == null ? "" : StringEscapeUtils.escapeHtml4(errorMessage);
    }
    
    public List<PageMetricModel> getPageTimeModels() {
        return super.getPageTimes().stream().map(p -> new PageMetricModel(p)).collect(Collectors.toList());
    }
    
    private static boolean isJexlQuery(Set<QueryImpl.Parameter> params) {
        return params.stream().anyMatch(p -> p.getParameterName().equals("query.syntax") && p.getParameterValue().equals("JEXL"));
    }
    
    private String numToString(long number, long minValue) {
        return number < minValue ? "" : nf.format(number);
    }
    
    private static String toFormattedParametersString(final Set<QueryImpl.Parameter> parameters) {
        final StringBuilder params = new StringBuilder();
        final String PARAMETER_SEPARATOR = ";";
        final String PARAMETER_NAME_VALUE_SEPARATOR = ":";
        final String NEWLINE = System.getProperty("line.separator");
        
        if (null != parameters) {
            for (final QueryImpl.Parameter param : parameters) {
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
    
    public class PageMetricModel extends PageMetric {
        
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        
        public PageMetricModel(PageMetric pageMetric) {
            super(pageMetric);
        }
        
        public String getPageNumberStr() {
            return numToString(getPageNumber(), 1);
        }
        
        public String getPageRequestedStr() {
            return getPageRequested() > 0 ? sdf.format(new Date(getPageRequested())) : "";
        }
        
        public String getPageReturnedStr() {
            return getPageReturned() > 0 ? sdf.format(new Date(getPageReturned())) : "";
        }
        
        public String getReturnTimeStr() {
            return nf.format(getReturnTime());
        }
        
        public String getPageSizeStr() {
            return nf.format(getPagesize());
        }
        
        public String getCallTimeStr() {
            return numToString(getCallTime(), 0);
        }
        
        public String getLoginTimeStr() {
            return numToString(getLoginTime(), 0);
        }
        
        public String getSerializationTimeStr() {
            return numToString(getSerializationTime(), 0);
        }
        
        public String getBytesWrittenStr() {
            return numToString(getBytesWritten(), 0);
        }
    }
}
