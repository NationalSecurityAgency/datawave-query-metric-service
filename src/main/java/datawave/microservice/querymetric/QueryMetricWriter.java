package datawave.microservice.querymetric;

import datawave.microservice.querymetric.config.QueryMetricTimelyProperties;
import datawave.microservice.querymetric.handler.QueryMetricHandler;
import datawave.util.timely.UdpClient;
import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.metric.BaseQueryMetric.Lifecycle;
import datawave.webservice.query.metric.BaseQueryMetric.PageMetric;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.endpoint.event.RefreshEvent;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.BeanManager;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class QueryMetricWriter {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @Autowired
    private QueryMetricHandler<? extends BaseQueryMetric> queryMetricHandler;
    
    @Autowired
    private QueryMetricTimelyProperties queryMetricTimelyProperties;
    
    private UdpClient timelyClient = null;
    private Map<String,Long> lastPageMetricMap;
    
    // queryId to lastPage Map
    private DecimalFormat df = new DecimalFormat("0.00");
    
    private UdpClient createUdpClient() {
        if (queryMetricTimelyProperties != null && StringUtils.isNotBlank(queryMetricTimelyProperties.getTimelyHost())) {
            return new UdpClient(queryMetricTimelyProperties.getTimelyHost(), queryMetricTimelyProperties.getTimelyPort());
        } else {
            return null;
        }
    }
    
    public void onRefresh(@Observes RefreshEvent event, BeanManager bm) {
        // protect timelyClient from being used in sendMetricsToTimely while re-creating the client
        synchronized (this) {
            timelyClient = createUdpClient();
        }
    }
    
    @PostConstruct
    private void init() {
        lastPageMetricMap = new LRUMap(1000);
        timelyClient = createUdpClient();
    }
    
    public synchronized void sendMetricsToTimely(BaseQueryMetric queryMetric) {
        
        if (timelyClient != null && queryMetric.getQueryType().equalsIgnoreCase("RunningQuery")) {
            try {
                String queryId = queryMetric.getQueryId();
                BaseQueryMetric.Lifecycle lifecycle = queryMetric.getLifecycle();
                Map<String,String> metricValues = queryMetricHandler.getEventFields(queryMetric);
                long createDate = queryMetric.getCreateDate().getTime();
                
                StringBuilder tagSb = new StringBuilder();
                Set<String> configuredMetricTags = queryMetricTimelyProperties.getTimelyMetricTags();
                for (String fieldName : configuredMetricTags) {
                    String fieldValue = metricValues.get(fieldName);
                    if (!StringUtils.isBlank(fieldValue)) {
                        // ensure that there are no spaces in tag values
                        fieldValue = fieldValue.replaceAll(" ", "_");
                        tagSb.append(fieldName).append("=").append(fieldValue).append(" ");
                    }
                }
                int tagSbLength = tagSb.length();
                if (tagSbLength > 0) {
                    if (tagSb.charAt(tagSbLength - 1) == ' ') {
                        tagSb.deleteCharAt(tagSbLength - 1);
                    }
                }
                tagSb.append("\n");
                
                timelyClient.open();
                
                if (lifecycle.equals(Lifecycle.RESULTS) || lifecycle.equals(Lifecycle.NEXTTIMEOUT) || lifecycle.equals(Lifecycle.MAXRESULTS)) {
                    List<PageMetric> pageTimes = queryMetric.getPageTimes();
                    // there should only be a maximum of one page metric as all but the last are removed by the QueryMetricsBean
                    for (PageMetric pm : pageTimes) {
                        Long lastPageSent = lastPageMetricMap.get(queryId);
                        // prevent duplicate reporting
                        if (lastPageSent == null || pm.getPageNumber() > lastPageSent) {
                            long requestTime = pm.getPageRequested();
                            long callTime = pm.getCallTime();
                            if (callTime == -1) {
                                callTime = pm.getReturnTime();
                            }
                            if (pm.getPagesize() > 0) {
                                timelyClient.write("put dw.query.metrics.PAGE_METRIC.calltime " + requestTime + " " + callTime + " " + tagSb);
                                String callTimePerRecord = df.format((double) callTime / pm.getPagesize());
                                timelyClient.write("put dw.query.metrics.PAGE_METRIC.calltimeperrecord " + requestTime + " " + callTimePerRecord + " " + tagSb);
                            }
                            lastPageMetricMap.put(queryId, pm.getPageNumber());
                            
                        }
                    }
                }
                
                if (lifecycle.equals(Lifecycle.CLOSED) || lifecycle.equals(Lifecycle.CANCELLED)) {
                    // write ELAPSED_TIME
                    timelyClient.write("put dw.query.metrics.ELAPSED_TIME " + createDate + " " + queryMetric.getElapsedTime() + " " + tagSb);
                    
                    // write NUM_RESULTS
                    timelyClient.write("put dw.query.metrics.NUM_RESULTS " + createDate + " " + queryMetric.getNumResults() + " " + tagSb);
                    
                    // clean up last page map
                    lastPageMetricMap.remove(queryId);
                }
                
                if (lifecycle.equals(Lifecycle.INITIALIZED)) {
                    // write CREATE_TIME
                    long createTime = queryMetric.getCreateCallTime();
                    if (createTime == -1) {
                        createTime = queryMetric.getSetupTime();
                    }
                    timelyClient.write("put dw.query.metrics.CREATE_TIME " + createDate + " " + createTime + " " + tagSb);
                    
                    // write a COUNT value of 1 so that we can count total queries
                    timelyClient.write("put dw.query.metrics.COUNT " + createDate + " 1 " + tagSb);
                }
                
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
