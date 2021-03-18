package datawave.microservice.querymetrics.handler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import datawave.microservice.querymetrics.config.QueryMetricHandlerProperties;
import datawave.query.iterator.QueryOptions;
import datawave.query.metrics.QueryMetricQueryLogic;
import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.cache.QueryMetricFactory;
import datawave.webservice.query.cache.QueryMetricFactoryImpl;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.metric.BaseQueryMetric.Lifecycle;
import datawave.webservice.query.metric.QueryMetric;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.query.util.QueryUtil;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.EventQueryResponseBase;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

@Component
public class QueryMetricReaderWriter {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(QueryMetricReaderWriter.class.getName());
    
    private static final String QUERY_METRICS_LOGIC_NAME = "QueryMetricsQuery";
    protected static final String DEFAULT_SECURITY_MARKING = "PUBLIC";
    private String connectorAuthorizations = null;
    private QueryMetricFactory metricFactory = new QueryMetricFactoryImpl();
    
    protected Connector connector;
    protected QueryMetricHandlerProperties queryMetricHandlerProperties;
    
    @Autowired
    public QueryMetricReaderWriter(QueryMetricHandlerProperties queryMetricHandlerProperties, @Qualifier("warehouse") Connector connector,
                    CacheManager cacheManager) {
        this.queryMetricHandlerProperties = queryMetricHandlerProperties;
        this.connector = connector;
    }
    
    private List<QueryMetric> getQueryMetrics(final String queryId) {
        Date end = new Date();
        Date begin = DateUtils.setYears(end, 2000);
        QueryImpl query = new QueryImpl();
        query.setBeginDate(begin);
        query.setEndDate(end);
        query.setQueryLogicName(QUERY_METRICS_LOGIC_NAME);
        query.setQuery("QUERY_ID == '" + queryId + "'");
        query.setQueryName(QUERY_METRICS_LOGIC_NAME);
        query.setColumnVisibility(queryMetricHandlerProperties.getVisibilityString());
        query.setQueryAuthorizations(connectorAuthorizations);
        // if (updatedQueryMetric.getUserDN() != null) {
        // query.setUserDN(updatedQueryMetric.getUserDN());
        // }
        query.setExpirationDate(DateUtils.addDays(new Date(), 1));
        query.setPagesize(1000);
        query.setId(UUID.randomUUID());
        query.setParameters(ImmutableMap.of(QueryOptions.INCLUDE_GROUPING_CONTEXT, "true"));
        return getQueryMetrics(null, query);
    }
    
    private List<QueryMetric> getQueryMetrics(BaseResponse response, Query query/* , DatawavePrincipal datawavePrincipal */) {
        List<QueryMetric> queryMetrics = new ArrayList<>();
        RunningQuery runningQuery;
        Connector connector = null;
        
        try {
            QueryLogic<?> queryLogic = new QueryMetricQueryLogic();
            // FIXME
            DatawavePrincipal datawavePrincipal = new DatawavePrincipal();
            runningQuery = new RunningQuery(null, connector, AccumuloConnectionFactory.Priority.ADMIN, queryLogic, query, query.getQueryAuthorizations(),
                            datawavePrincipal, metricFactory);
            
            boolean done = false;
            List<Object> objectList = new ArrayList<>();
            
            while (!done) {
                ResultsPage resultsPage = runningQuery.next();
                
                if (!resultsPage.getResults().isEmpty()) {
                    objectList.addAll(resultsPage.getResults());
                } else {
                    done = true;
                }
            }
            
            BaseQueryResponse queryResponse = queryLogic.getTransformer(query).createResponse(new ResultsPage(objectList));
            List<QueryExceptionType> exceptions = queryResponse.getExceptions();
            
            if (queryResponse.getExceptions() != null && !queryResponse.getExceptions().isEmpty()) {
                if (response != null) {
                    response.setExceptions(new LinkedList<>(exceptions));
                    response.setHasResults(false);
                }
            }
            
            if (!(queryResponse instanceof EventQueryResponseBase)) {
                if (response != null) {
                    response.addException(new QueryException("incompatible response")); // TODO: Should this be an IllegalStateException?
                    response.setHasResults(false);
                }
            }
            
            EventQueryResponseBase eventQueryResponse = (EventQueryResponseBase) queryResponse;
            List<EventBase> eventList = eventQueryResponse.getEvents();
            
            for (EventBase<?,?> event : eventList) {
                QueryMetric metric = toMetric(event);
                queryMetrics.add(metric);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            if (response != null) {
                response.addExceptions(new QueryException(e).getQueryExceptionsInStack());
            }
        } finally {
            // if (null != this.connectionFactory) {
            // if (null != runningQuery && null != connector) {
            // try {
            // runningQuery.closeConnection(this.connectionFactory);
            // } catch (Exception e) {
            // log.warn("Could not return connector to factory", e);
            // }
            // } else if (null != connector) {
            // try {
            // this.connectionFactory.returnConnection(connector);
            // } catch (Exception e) {
            // log.warn("Could not return connector to factory", e);
            // }
            // }
            // }
        }
        
        return queryMetrics;
    }
    
    public QueryMetric toMetric(datawave.webservice.query.result.event.EventBase event) {
        SimpleDateFormat sdf_date_time1 = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat sdf_date_time2 = new SimpleDateFormat("yyyyMMdd HHmmss");
        
        try {
            QueryMetric m = new QueryMetric();
            List<FieldBase> field = event.getFields();
            
            TreeMap<Long,BaseQueryMetric.PageMetric> pageMetrics = Maps.newTreeMap();
            
            for (FieldBase f : field) {
                String fieldName = f.getName();
                String fieldValue = f.getValueString();
                
                if (fieldName.equals("USER")) {
                    m.setUser(fieldValue);
                } else if (fieldName.equals("USER_DN")) {
                    m.setUserDN(fieldValue);
                } else if (fieldName.equals("QUERY_ID")) {
                    m.setQueryId(fieldValue);
                } else if (fieldName.equals("CREATE_DATE")) {
                    try {
                        Date d = sdf_date_time2.parse(fieldValue);
                        m.setCreateDate(d);
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                } else if (fieldName.equals("QUERY")) {
                    m.setQuery(fieldValue);
                } else if (fieldName.equals("PLAN")) {
                    m.setPlan(fieldValue);
                } else if (fieldName.equals("QUERY_LOGIC")) {
                    m.setQueryLogic(fieldValue);
                } else if (fieldName.equals("QUERY_ID")) {
                    m.setQueryId(fieldValue);
                } else if (fieldName.equals("BEGIN_DATE")) {
                    try {
                        Date d = sdf_date_time1.parse(fieldValue);
                        m.setBeginDate(d);
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                } else if (fieldName.equals("END_DATE")) {
                    try {
                        Date d = sdf_date_time1.parse(fieldValue);
                        m.setEndDate(d);
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                } else if (fieldName.equals("HOST")) {
                    m.setHost(fieldValue);
                } else if (fieldName.equals("PROXY_SERVERS")) {
                    m.setProxyServers(Arrays.asList(StringUtils.split(fieldValue, ",")));
                } else if (fieldName.equals("AUTHORIZATIONS")) {
                    m.setQueryAuthorizations(fieldValue);
                } else if (fieldName.equals("QUERY_TYPE")) {
                    m.setQueryType(fieldValue);
                } else if (fieldName.equals("LIFECYCLE")) {
                    m.setLifecycle(Lifecycle.valueOf(fieldValue));
                } else if (fieldName.equals("ERROR_CODE")) {
                    m.setErrorCode(fieldValue);
                } else if (fieldName.equals("ERROR_MESSAGE")) {
                    m.setErrorMessage(fieldValue);
                } else if (fieldName.equals("SETUP_TIME")) {
                    m.setSetupTime(Long.parseLong(fieldValue));
                } else if (fieldName.equals("CREATE_CALL_TIME")) {
                    m.setCreateCallTime(Long.parseLong(fieldValue));
                } else if (fieldName.startsWith("PAGE_METRICS")) {
                    int index = fieldName.indexOf(".");
                    if (-1 == index) {
                        log.error("Could not parse field name to extract repetition count: " + fieldName);
                    } else {
                        Long repetition = Long.parseLong(fieldName.substring(index + 1));
                        
                        String[] parts = StringUtils.split(fieldValue, "/");
                        BaseQueryMetric.PageMetric pageMetric = null;
                        if (parts.length == 8) {
                            pageMetric = new BaseQueryMetric.PageMetric(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]),
                                            Long.parseLong(parts[3]), Long.parseLong(parts[4]), Long.parseLong(parts[5]), Long.parseLong(parts[6]),
                                            Long.parseLong(parts[7]));
                        } else if (parts.length == 7) {
                            pageMetric = new BaseQueryMetric.PageMetric(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]),
                                            Long.parseLong(parts[3]), Long.parseLong(parts[4]), Long.parseLong(parts[5]), Long.parseLong(parts[6]));
                        } else if (parts.length == 5) {
                            pageMetric = new BaseQueryMetric.PageMetric(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]),
                                            Long.parseLong(parts[3]), Long.parseLong(parts[4]), 0l, 0l);
                        } else if (parts.length == 2) {
                            pageMetric = new BaseQueryMetric.PageMetric(Long.parseLong(parts[0]), Long.parseLong(parts[1]), 0l, 0l);
                        }
                        
                        if (pageMetric != null)
                            pageMetrics.put(repetition, pageMetric);
                    }
                } else if (fieldName.equals("POSITIVE_SELECTORS")) {
                    List<String> positiveSelectors = m.getPositiveSelectors();
                    if (positiveSelectors == null) {
                        positiveSelectors = new ArrayList<>();
                    }
                    positiveSelectors.add(fieldValue);
                    m.setPositiveSelectors(positiveSelectors);
                } else if (fieldName.equals("NEGATIVE_SELECTORS")) {
                    List<String> negativeSelectors = m.getNegativeSelectors();
                    if (negativeSelectors == null) {
                        negativeSelectors = new ArrayList<>();
                    }
                    negativeSelectors.add(fieldValue);
                    m.setNegativeSelectors(negativeSelectors);
                } else if (fieldName.equals("LAST_UPDATED")) {
                    try {
                        Date d = sdf_date_time2.parse(fieldValue);
                        m.setLastUpdated(d);
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                } else if (fieldName.equals("NUM_UPDATES")) {
                    try {
                        long numUpdates = Long.parseLong(fieldValue);
                        m.setNumUpdates(numUpdates);
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                } else if (fieldName.equals("QUERY_NAME")) {
                    m.setQueryName(fieldValue);
                } else if (fieldName.equals("PARAMETERS")) {
                    if (fieldValue != null) {
                        try {
                            Set<QueryImpl.Parameter> parameters = QueryUtil.parseParameters(fieldValue);
                            m.setParameters(parameters);
                            
                        } catch (Exception e) {
                            log.debug(e.getMessage());
                        }
                    }
                }
                
                else if (fieldName.equals("SOURCE_COUNT")) {
                    m.setSourceCount(Long.parseLong(fieldValue));
                }
                
                else if (fieldName.equals("NEXT_COUNT")) {
                    m.setNextCount(Long.parseLong(fieldValue));
                }
                
                else if (fieldName.equals("SEEK_COUNT")) {
                    m.setSeekCount(Long.parseLong(fieldValue));
                }
                
                else if (fieldName.equals("YIELD_COUNT")) {
                    m.setYieldCount(Long.parseLong(fieldValue));
                }
                
                else if (fieldName.equals("DOC_RANGES")) {
                    m.setDocRanges(Long.parseLong(fieldValue));
                }
                
                else if (fieldName.equals("FI_RANGES")) {
                    m.setFiRanges(Long.parseLong(fieldValue));
                } else {
                    log.error("encountered unanticipated field name: " + fieldName);
                }
            }
            
            for (final Map.Entry<Long,BaseQueryMetric.PageMetric> entry : pageMetrics.entrySet())
                m.addPageMetric(entry.getValue());
            
            return m;
        } catch (RuntimeException e) {
            return null;
        }
    }
}
