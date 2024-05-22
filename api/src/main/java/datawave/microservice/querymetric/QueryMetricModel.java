package datawave.microservice.querymetric;

import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;

public class QueryMetricModel extends QueryMetric implements QueryMetricModelFormat {
    
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
        return getPredictionsStr(predictions);
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
    
    // will most likely have to do something like the iteration that predictions uses
    public String getSubPlanStr() {
        return nf.format(sourceCount);
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
}
