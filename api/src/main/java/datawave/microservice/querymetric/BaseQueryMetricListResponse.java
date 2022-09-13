package datawave.microservice.querymetric;

import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.webservice.result.BaseResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.springframework.web.servlet.ModelAndView;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

public abstract class BaseQueryMetricListResponse<T extends BaseQueryMetric> extends BaseResponse {
    
    private static final long serialVersionUID = 1L;
    @XmlElementWrapper(name = "queryMetrics")
    @XmlElement(name = "queryMetric")
    protected List<T> result = null;
    @XmlElement
    protected int numResults = 0;
    @XmlTransient
    private boolean administratorMode = false;
    @XmlTransient
    private boolean isGeoQuery = false;
    
    private static String numToString(long number) {
        return (number == -1 || number == 0) ? "" : Long.toString(number);
    }
    
    public List<T> getResult() {
        return result;
    }
    
    public int getNumResults() {
        return numResults;
    }
    
    public void setResult(List<T> result) {
        this.result = result;
        this.numResults = this.result.size();
    }
    
    public void setNumResults(int numResults) {
        this.numResults = numResults;
    }
    
    public boolean isAdministratorMode() {
        return administratorMode;
    }
    
    public void setAdministratorMode(boolean administratorMode) {
        this.administratorMode = administratorMode;
    }
    
    public boolean isGeoQuery() {
        return isGeoQuery;
    }
    
    public void setGeoQuery(boolean geoQuery) {
        isGeoQuery = geoQuery;
    }
    
    /**
     * Creates the ModelAndView for the base query metrics page (basequerymetrics.html)
     * 
     * @return the ModelAndView for basequerymetrics.html
     */
    public ModelAndView createModelAndView() {
        ModelAndView mav = new ModelAndView();
        
        mav.setViewName("basequerymetrics");
        
        TreeMap<Date,T> metricMap = new TreeMap<Date,T>(Collections.reverseOrder());
        
        for (T metric : this.getResult()) {
            metricMap.put(metric.getCreateDate(), metric);
        }
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        
        List<List<String>> metricsTableContent = new ArrayList<List<String>>();
        for (T metric : metricMap.values()) {
            List<String> metricsData = new ArrayList<String>();
            
            metricsData.add(metric.getColumnVisibility());
            metricsData.add(sdf.format(metric.getCreateDate()));
            metricsData.add(metric.getUser());
            String userDN = metric.getUserDN();
            metricsData.add(userDN == null ? "" : userDN);
            String proxyServers = metric.getProxyServers() == null ? "" : StringUtils.join(metric.getProxyServers(), "<BR/>");
            metricsData.add(proxyServers);
            String url;
            if (this.isAdministratorMode()) {
                url = "/DataWave/Query/Metrics/user/" + metric.getUser() + "/" + metric.getQueryId() + "/";
            } else {
                url = "/DataWave/Query/Metrics/id/" + metric.getQueryId() + "/";
            }
            metricsData.add(url);
            metricsData.add(metric.getQueryId());
            metricsData.add(metric.getQueryType());
            metricsData.add(metric.getQueryLogic());
            metricsData.add(StringEscapeUtils.escapeHtml4(metric.getQuery()));
            String beginDate = metric.getBeginDate() == null ? "" : sdf.format(metric.getBeginDate());
            metricsData.add(beginDate);
            String endDate = metric.getEndDate() == null ? "" : sdf.format(metric.getEndDate());
            metricsData.add(endDate);
            String queryAuths = metric.getQueryAuthorizations() == null ? "" : metric.getQueryAuthorizations().replaceAll(",", " ");
            metricsData.add(queryAuths);
            metricsData.add(metric.getHost());
            metricsData.add(String.valueOf(metric.getSetupTime()));
            metricsData.add(numToString(metric.getCreateCallTime()));
            metricsData.add(String.valueOf(metric.getNumPages()));
            metricsData.add(String.valueOf(metric.getNumResults()));
            long count = 0l;
            long callTime = 0l;
            long serializationTime = 0l;
            long bytesSent = 0l;
            for (PageMetric p : metric.getPageTimes()) {
                count += p.getReturnTime();
                callTime += (p.getCallTime()) == -1 ? 0 : p.getCallTime();
                serializationTime += (p.getSerializationTime()) == -1 ? 0 : p.getSerializationTime();
                bytesSent += (p.getBytesWritten()) == -1 ? 0 : p.getBytesWritten();
            }
            metricsData.add(String.valueOf(count));
            metricsData.add(numToString(callTime));
            metricsData.add(numToString(serializationTime));
            metricsData.add(numToString(bytesSent));
            metricsData.add(String.valueOf(metric.getLifecycle()));
            metricsData.add(String.valueOf(metric.getElapsedTime()));
            String errorCode = metric.getErrorCode();
            metricsData.add((errorCode == null) ? "" : StringEscapeUtils.escapeHtml4(errorCode));
            String errorMessage = metric.getErrorMessage();
            metricsData.add((errorMessage == null) ? "" : StringEscapeUtils.escapeHtml4(errorMessage));
            
            metricsTableContent.add(metricsData);
        }
        
        mav.addObject("isGeoQuery", this.isGeoQuery());
        mav.addObject("metricsTableContent", metricsTableContent);
        
        return mav;
    }
}
