package datawave.microservice.querymetric;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.bind.annotation.XmlTransient;

import org.apache.commons.text.StringEscapeUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

import datawave.webservice.query.QueryImpl;

public abstract class BaseQueryMetricSubplanResponse<T extends BaseQueryMetric> extends BaseQueryMetricListResponse<T> {
    
    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Query Metrics / Subplans";
    
    @Override
    public String getTitle() {
        return TITLE;
    }
    
    @JsonIgnore
    @XmlTransient
    @Override
    public String getPageHeader() {
        return getTitle();
    }
    
    @JsonIgnore
    @XmlTransient
    
    @Override
    public String getMainContent() {
        TreeMap<Date,T> metricMap = new TreeMap<>(Collections.reverseOrder());
        for (T metric : this.getResult()) {
            metricMap.put(metric.getCreateDate(), metric);
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        StringBuilder builder = new StringBuilder();
        int x = 0;
        for (T metric : metricMap.values()) {
            builder.append("<table>\n");
            builder.append("<tr>");
            builder.append("<th>Visibility</th>");
            builder.append("<th>Query Date</th>");
            builder.append("<th>User</th>");
            builder.append("<th>UserDN</th>");
            builder.append("<th>Query ID</th>");
            builder.append("<th>Query Logic</th>");
            builder.append("<th id=\"query-header\">Query</th>");
            builder.append("<th>Query Plan</th>");
            builder.append("</tr>");
            
            Set<QueryImpl.Parameter> parameters = metric.getParameters();
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
            if (this.isAdministratorMode()) {
                builder.append("<td><a href=\"" + BASE_URL + "/user/").append(metric.getUser()).append("/").append(metric.getQueryId()).append("/")
                                .append("\">").append(metric.getQueryId()).append("</a></td>");
            } else {
                builder.append("<td><a href=\"" + BASE_URL + "/id/").append(metric.getQueryId()).append("/").append("\">").append(metric.getQueryId())
                                .append("</a></td>");
            }
            builder.append("<td>").append(metric.getQueryLogic()).append("</td>");
            builder.append(isJexlQuery(parameters) ? "<td style=\"white-space: pre; word-wrap: break-word;\">" : "<td style=\"word-wrap: break-word;\">")
                            .append(StringEscapeUtils.escapeHtml4(metric.getQuery())).append("</td>");
            builder.append("<td style=\"white-space: pre; word-wrap: break-word;\">").append(StringEscapeUtils.escapeHtml4(metric.getPlan())).append("</td>");
            builder.append("</tr>\n");
            builder.append("</table>\n");
            
            builder.append("<br/>");
            builder.append("<table>\n");
            builder.append("<tr><th>Range</th><th>Sub Plan</th></tr>");
            if (metric.getSubPlans() != null && !metric.getSubPlans().isEmpty()) {
                int s = 0;
                for (Map.Entry<String,RangeCounts> e : metric.getSubPlans().entrySet()) {
                    // highlight alternating rows
                    if (s % 2 == 0) {
                        builder.append("<tr class=\"highlight\">");
                    } else {
                        builder.append("<tr>");
                    }
                    builder.append("<td>").append(e.getKey()).append("</td>");
                    builder.append("<td>").append("[" + e.getValue().getDocumentRangeCount() + "," + e.getValue().getShardRangeCount() + "]").append("</td>");
                    builder.append("\n</tr>\n");
                    s++;
                }
            } else {
                builder.append("<tr><td colspan=\"2\">NONE<td/></tr>");
            }
            builder.append("</td></tr>\n");
            builder.append("</table>\n");
            builder.append("\n<br/><br/>\n");
        }
        return builder.toString();
    }
    
    private static boolean isJexlQuery(Set<QueryImpl.Parameter> params) {
        return params.stream().anyMatch(p -> p.getParameterName().equals("query.syntax") && p.getParameterValue().equals("JEXL"));
    }
}
