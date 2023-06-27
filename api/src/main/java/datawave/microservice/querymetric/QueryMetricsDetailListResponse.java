package datawave.microservice.querymetric;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;

import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;
import datawave.webservice.query.QueryImpl.Parameter;

@XmlRootElement(name = "QueryMetricListResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryMetricsDetailListResponse extends QueryMetricListResponse {
    
    private static final long serialVersionUID = 1L;
    private static final String NEWLINE = System.getProperty("line.separator");
    
    @Override
    public String getHeadContent() {
        return super.getHeadContent() + "<link rel=\"stylesheet\" type=\"text/css\" href=\"/querymetric/css/theme.css\">"; // styling for microservices query
                                                                                                                           // metric page
    }
    
    @Override
    public String getMainContent() {
        StringBuilder builder = new StringBuilder(), pageTimesBuilder = new StringBuilder();
        
        builder.append("<table>\n");
        builder.append("<tr>");
        builder.append("<th>Visibility</th><th>Query Date</th><th>User</th><th>UserDN</th><th>Proxy Server(s)</th><th>Query ID</th><th>Query Type</th>");
        builder.append("<th>Query Logic</th><th id=\"query-header\">Query</th><th>Query Plan</th><th>Query Name</th><th>Begin Date</th><th>End Date</th><th>Parameters</th><th>Query Auths</th>");
        builder.append("<th>Server</th>");
        builder.append("<th>Predictions</th>");
        builder.append("<th>Login Time (ms)</th>");
        builder.append("<th>Query Setup Time (ms)</th><th>Query Setup Call Time (ms)</th><th>Number Pages</th><th>Number Results</th>");
        
        builder.append("<th>Doc Ranges</th><th>FI Ranges</th>");
        builder.append("<th>Sources</th><th>Next Calls</th><th>Seek Calls</th><th>Yield Count</th><th>Versions</th>");
        
        builder.append("<th>Total Page Time (ms)</th><th>Total Page Call Time (ms)</th><th>Total Page Serialization Time (ms)</th>");
        builder.append("<th>Total Page Bytes Sent (uncompressed)</th><th>Lifecycle</th><th>Elapsed Time</th><th>Error Code</th><th>Error Message</th>");
        builder.append("</tr>\n");
        
        pageTimesBuilder.append("<table>\n");
        pageTimesBuilder.append("<tr><th>Page number</th><th>Page requested</th><th>Page returned</th><th>Response time (ms)</th>");
        pageTimesBuilder.append("<th>Page size</th><th>Call time (ms)</th><th>Login time (ms)</th><th>Serialization time (ms)</th>");
        pageTimesBuilder.append("<th>Bytes written (uncompressed)</th></tr>");
        
        TreeMap<Date,QueryMetric> metricMap = new TreeMap<>(Collections.reverseOrder());
        
        for (QueryMetric metric : this.getResult()) {
            metricMap.put(metric.getCreateDate(), metric);
        }
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        
        int x = 0;
        for (QueryMetric metric : metricMap.values()) {
            Set<Parameter> parameters = metric.getParameters();
            
            // highlight alternating rows
            if (x % 2 == 0) {
                builder.append("<tr class=\"highlight\">\n");
            } else {
                builder.append("<tr>\n");
            }
            x++;
            
            builder.append("<td>").append(metric.getColumnVisibility()).append("</td>");
            builder.append("<td style=\"min-width:125px !important;\">").append(sdf.format(metric.getCreateDate())).append("</td>");
            builder.append("<td>").append(metric.getUser()).append("</td>");
            String userDN = metric.getUserDN();
            builder.append("<td style=\"min-width:500px !important;\">").append(userDN == null ? "" : userDN).append("</td>");
            String proxyServers = metric.getProxyServers() == null ? "" : StringUtils.join(metric.getProxyServers(), "<BR/>");
            builder.append("<td>").append(proxyServers).append("</td>");
            builder.append("<td>").append(metric.getQueryId()).append("</td>");
            builder.append("<td>").append(metric.getQueryType()).append("</td>");
            builder.append("<td>").append(metric.getQueryLogic()).append("</td>");
            // Note the query and query plan are added to the table later (see the javascript at the end of this for loop)
            builder.append(isJexlQuery(parameters) ? "<td id='query" + x + "'" + " style=\"white-space: pre; word-wrap: break-word;\">"
                            : "<td id='query" + x + "'" + " style=\"word-wrap: break-word;\">").append("</td>");
            builder.append("<td id='query-plan" + x + "'" + " style=\"white-space: pre; word-wrap: break-word;\">").append("</td>");
            builder.append("<td>").append(metric.getQueryName()).append("</td>");
            
            String beginDate = metric.getBeginDate() == null ? "" : sdf.format(metric.getBeginDate());
            builder.append("<td style=\"min-width:125px !important;\">").append(beginDate).append("</td>");
            String endDate = metric.getEndDate() == null ? "" : sdf.format(metric.getEndDate());
            builder.append("<td style=\"min-width:125px !important;\">").append(endDate).append("</td>");
            builder.append("<td style=\"white-space: pre; word-wrap: break-word;\">").append(parameters == null ? "" : toFormattedParametersString(parameters))
                            .append("</td>");
            String queryAuths = metric.getQueryAuthorizations() == null ? "" : metric.getQueryAuthorizations().replaceAll(",", " ");
            builder.append("<td style=\"word-wrap: break-word; min-width:300px !important;\">").append(queryAuths).append("</td>");
            
            builder.append("<td>").append(metric.getHost()).append("</td>");
            if (metric.getPredictions() != null && !metric.getPredictions().isEmpty()) {
                builder.append("<td>");
                String delimiter = "";
                List<Prediction> predictions = new ArrayList<>(metric.getPredictions());
                Collections.sort(predictions);
                for (Prediction prediction : predictions) {
                    builder.append(delimiter).append(prediction.getName()).append(" = ").append(prediction.getPrediction());
                    delimiter = "<br>";
                }
            } else {
                builder.append("<td/>");
            }
            builder.append("<td>").append(numToString(metric.getLoginTime(), 0)).append("</td>");
            builder.append("<td>").append(metric.getSetupTime()).append("</td>");
            builder.append("<td>").append(numToString(metric.getCreateCallTime(), 0)).append("</td>\n");
            builder.append("<td>").append(metric.getNumPages()).append("</td>");
            builder.append("<td>").append(metric.getNumResults()).append("</td>");
            
            builder.append("<td>").append(metric.getDocRanges()).append("</td>");
            builder.append("<td>").append(metric.getFiRanges()).append("</td>");
            
            builder.append("<td>").append(metric.getSourceCount()).append("</td>");
            builder.append("<td>").append(metric.getNextCount()).append("</td>");
            builder.append("<td>").append(metric.getSeekCount()).append("</td>");
            builder.append("<td>").append(metric.getYieldCount()).append("</td>");
            String versions = metric.getVersionMap().entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("<br/>"));
            builder.append("<td style=\"min-width:250px !important;\">").append(versions).append("</td>");
            
            long count = 0l;
            long callTime = 0l;
            long serializationTime = 0l;
            long bytesSent = 0l;
            int y = 0;
            for (PageMetric p : metric.getPageTimes()) {
                count += p.getReturnTime();
                callTime += (p.getCallTime()) == -1 ? 0 : p.getCallTime();
                serializationTime += (p.getSerializationTime()) == -1 ? 0 : p.getSerializationTime();
                bytesSent += (p.getBytesWritten()) == -1 ? 0 : p.getBytesWritten();
                if (y % 2 == 0) {
                    pageTimesBuilder.append("<tr class=\"highlight\">");
                } else {
                    pageTimesBuilder.append("<tr>");
                }
                y++;
                
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
                
                pageTimesBuilder.append("<td>").append(numToString(p.getPageNumber(), 1)).append("</td>");
                pageTimesBuilder.append("<td>").append(pageRequestedStr).append("</td>");
                pageTimesBuilder.append("<td>").append(pageReturnedStr).append("</td>");
                pageTimesBuilder.append("<td>").append(p.getReturnTime()).append("</td>");
                pageTimesBuilder.append("<td>").append(p.getPagesize()).append("</td>");
                pageTimesBuilder.append("<td>").append(numToString(p.getCallTime(), 0)).append("</td>");
                pageTimesBuilder.append("<td>").append(numToString(p.getLoginTime(), 0)).append("</td>");
                pageTimesBuilder.append("<td>").append(numToString(p.getSerializationTime(), 0)).append("</td>");
                pageTimesBuilder.append("<td>").append(numToString(p.getBytesWritten(), 0)).append("</td></tr>");
            }
            builder.append("<td>").append(count).append("</td>\n");
            builder.append("<td>").append(numToString(callTime, 0)).append("</td>\n");
            builder.append("<td>").append(numToString(serializationTime, 0)).append("</td>\n");
            builder.append("<td>").append(numToString(bytesSent, 0)).append("</td>\n");
            builder.append("<td>").append(metric.getLifecycle()).append("</td>");
            builder.append("<td>").append(metric.getElapsedTime()).append("</td>");
            String errorCode = metric.getErrorCode();
            builder.append("<td style=\"word-wrap: break-word;\">").append((errorCode == null) ? "" : StringEscapeUtils.escapeHtml4(errorCode)).append("</td>");
            String errorMessage = metric.getErrorMessage();
            builder.append("<td style=\"word-wrap: break-word;\">").append((errorMessage == null) ? "" : StringEscapeUtils.escapeHtml4(errorMessage))
                            .append("</td>");
            builder.append("\n</tr>\n");
            
            /*
             * javascript to make the metric's query and the metric's query plan interactive (highlight matching parens on mouse over, clicking a paren brings
             * you to its matching paren)
             */
            builder.append("<script>");
            // To be used in the query-interactive-parens.js file
            builder.append("function getQuery() { return `" + metric.getQuery() + "`; }" + NEWLINE);
            builder.append("function getPlan() { return `" + metric.getPlan() + "`; }" + NEWLINE);
            builder.append("function getMetricNum() { return " + x + "; }" + NEWLINE);
            builder.append("</script>");
            builder.append("<script src='/querymetric/js/query-interactive-parens.js'></script>"); // for microservices query metrics page
            builder.append("<script src='/query-interactive-parens.js'></script>"); // for webservers query metric page
        }
        
        builder.append("</table>\n<br/>\n");
        pageTimesBuilder.append("</table>\n");
        
        builder.append(pageTimesBuilder);
        
        return builder.toString();
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
