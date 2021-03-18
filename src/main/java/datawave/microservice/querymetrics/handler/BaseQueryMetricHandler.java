package datawave.microservice.querymetrics.handler;

import datawave.webservice.query.metric.BaseQueryMetric;
//import datawave.microservice.querymetrics.BaseQueryMetric.PageMetric;
//import datawave.microservice.querymetrics.handler.QueryMetricHandler;
//import datawave.webservice.query.metric.QueryMetricSummary;
//import datawave.webservice.query.metric.QueryMetricsSummaryHtmlResponse;
//import datawave.webservice.query.metric.QueryMetricsSummaryResponse;
//import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * 
 */
public abstract class BaseQueryMetricHandler<T extends BaseQueryMetric> implements QueryMetricHandler<T> {
    
    private Logger log = Logger.getLogger(BaseQueryMetricHandler.class);
    
}
