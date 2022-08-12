package datawave.microservice.querymetric;

import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;
import datawave.webservice.query.QueryImpl.Parameter;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.springframework.web.servlet.ModelAndView;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

@XmlRootElement(name = "QueryMetricListResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryMetricsDetailListResponse extends QueryMetricListResponse {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Creates the ModelAndView for the detailed query metrics page (detailedquerymetrics.html)
     * 
     * @return the ModelAndView for detailedquerymetrics.html
     */
    @Override
    public ModelAndView createModelAndView() {
        ModelAndView mav = new ModelAndView();
        
        mav.setViewName("detailedquerymetrics");
        
        TreeMap<Date,QueryMetric> metricMap = new TreeMap<Date,QueryMetric>(Collections.reverseOrder());
        
        for (QueryMetric metric : this.getResult()) {
            metricMap.put(metric.getCreateDate(), metric);
        }
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        
        List<List<String>> metricsTableContent = new ArrayList<List<String>>();
        List<List<String>> pageTimesTableContent = new ArrayList<List<String>>();
        for (QueryMetric metric : metricMap.values()) {
            List<String> metricsData = new ArrayList<String>();
            Set<datawave.webservice.query.QueryImpl.Parameter> parameters = metric.getParameters();
            
            metricsData.add(metric.getColumnVisibility());
            metricsData.add(sdf.format(metric.getCreateDate()));
            metricsData.add(metric.getUser());
            String userDN = metric.getUserDN();
            metricsData.add(userDN == null ? "" : userDN);
            String proxyServers = metric.getProxyServers() == null ? "" : StringUtils.join(metric.getProxyServers(), "<BR/>");
            metricsData.add(proxyServers);
            metricsData.add(metric.getQueryId());
            metricsData.add(metric.getQueryType());
            metricsData.add(metric.getQueryLogic());
            String queryStyle = isJexlQuery(parameters) ? "white-space: pre; word-wrap: break-word;" : "word-wrap: break-word;";
            metricsData.add(queryStyle);
            metricsData.add(metric.getQuery());
            metricsData.add(metric.getPlan());
            metricsData.add(metric.getQueryName());
            String beginDate = metric.getBeginDate() == null ? "" : sdf.format(metric.getBeginDate());
            metricsData.add(beginDate);
            String endDate = metric.getEndDate() == null ? "" : sdf.format(metric.getEndDate());
            metricsData.add(endDate);
            metricsData.add(parameters == null ? "" : toFormattedParametersString(parameters));
            String queryAuths = metric.getQueryAuthorizations() == null ? "" : metric.getQueryAuthorizations().replaceAll(",", " ");
            metricsData.add(queryAuths);
            metricsData.add(metric.getHost());
            
            StringBuilder sbPredictions = new StringBuilder();
            if (metric.getPredictions() != null && !metric.getPredictions().isEmpty()) {
                String delimiter = "";
                List<Prediction> predictions = new ArrayList<Prediction>(metric.getPredictions());
                Collections.sort(predictions);
                for (Prediction prediction : predictions) {
                    sbPredictions.append(delimiter).append(prediction.getName()).append(" = ").append(prediction.getPrediction());
                    delimiter = "<br>";
                }
            } else {
                sbPredictions.append("");
            }
            metricsData.add(sbPredictions.toString());
            
            metricsData.add(numToString(metric.getLoginTime(), 0));
            metricsData.add(String.valueOf(metric.getSetupTime()));
            metricsData.add(numToString(metric.getCreateCallTime(), 0));
            metricsData.add(String.valueOf(metric.getNumPages()));
            metricsData.add(String.valueOf(metric.getNumResults()));
            metricsData.add(String.valueOf(metric.getDocRanges()));
            metricsData.add(String.valueOf(metric.getFiRanges()));
            metricsData.add(String.valueOf(metric.getSourceCount()));
            metricsData.add(String.valueOf(metric.getNextCount()));
            metricsData.add(String.valueOf(metric.getSeekCount()));
            metricsData.add(String.valueOf(metric.getYieldCount()));
            metricsData.add(metric.getVersion());
            
            long count = 0l;
            long callTime = 0l;
            long serializationTime = 0l;
            long bytesSent = 0l;
            for (PageMetric p : metric.getPageTimes()) {
                List<String> pageTimesData = new ArrayList<String>();
                
                count += p.getReturnTime();
                callTime += (p.getCallTime()) == -1 ? 0 : p.getCallTime();
                serializationTime += (p.getSerializationTime()) == -1 ? 0 : p.getSerializationTime();
                bytesSent += (p.getBytesWritten()) == -1 ? 0 : p.getBytesWritten();
                
                String pageRequestedStr = "";
                long pageRequested = p.getPageRequested();
                if (pageRequested > 0) {
                    pageRequestedStr = sdf.format(new Date(pageRequested));
                }
                
                String pageReturnedStr = "";
                long pageReturned = p.getPageReturned();
                if (pageReturned > 0) {
                    pageReturnedStr = sdf.format(new Date(pageReturned));
                }
                
                pageTimesData.add(numToString(p.getPageNumber(), 1));
                pageTimesData.add(pageRequestedStr);
                pageTimesData.add(pageReturnedStr);
                pageTimesData.add(String.valueOf(p.getReturnTime()));
                pageTimesData.add(String.valueOf(p.getPagesize()));
                pageTimesData.add(numToString(p.getCallTime(), 0));
                pageTimesData.add(numToString(p.getLoginTime(), 0));
                pageTimesData.add(numToString(p.getSerializationTime(), 0));
                pageTimesData.add(numToString(p.getBytesWritten(), 0));
                
                pageTimesTableContent.add(pageTimesData);
            }
            
            metricsData.add(String.valueOf(count));
            metricsData.add(numToString(callTime, 0));
            metricsData.add(numToString(serializationTime, 0));
            metricsData.add(numToString(bytesSent, 0));
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
        mav.addObject("pageTimesTableContent", pageTimesTableContent);
        
        return mav;
    }
    
    private static boolean isJexlQuery(Set<Parameter> params) {
        return params.stream().anyMatch(p -> p.getParameterName().equals("query.syntax") && p.getParameterValue().equals("JEXL"));
    }
    
    private static String numToString(long number, long minValue) {
        return number < minValue ? "" : Long.toString(number);
    }
    
    private static String toFormattedParametersString(final Set<Parameter> parameters) {
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
