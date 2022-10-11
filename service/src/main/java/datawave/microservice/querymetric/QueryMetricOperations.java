package datawave.microservice.querymetric;

import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.marking.MarkingFunctions;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.querymetric.BaseQueryMetric.Lifecycle;
import datawave.microservice.querymetric.config.TimelyProperties;
import datawave.microservice.querymetric.factory.BaseQueryMetricListResponseFactory;
import datawave.microservice.querymetric.function.QueryMetricSupplier;
import datawave.microservice.querymetric.handler.QueryGeometryHandler;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.handler.SimpleQueryGeometryHandler;
import datawave.microservice.querymetric.stats.CacheStats;
import datawave.microservice.security.util.DnUtils;
import datawave.query.jexl.visitors.JexlFormattedStringBuildingVisitor;
import datawave.security.authorization.DatawaveUser;
import datawave.util.timely.UdpClient;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.map.QueryGeometryResponse;
import datawave.webservice.result.VoidResponse;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.http.MediaType;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.PermitAll;
import javax.inject.Named;
import java.net.InetAddress;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static datawave.microservice.querymetric.QueryMetricOperations.DEFAULT_DATETIME.BEGIN;
import static datawave.microservice.querymetric.QueryMetricOperations.DEFAULT_DATETIME.END;
import static datawave.microservice.querymetric.config.HazelcastMetricCacheConfiguration.INCOMING_METRICS;
import static datawave.microservice.querymetric.config.HazelcastMetricCacheConfiguration.LAST_WRITTEN_METRICS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * The type Query metric operations.
 */
@Tag(name = "Query Metric Operations /v1",
                externalDocs = @ExternalDocumentation(description = "Query Metric Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-query-metric-service"))
@RestController
@RequestMapping(path = "/v1")
public class QueryMetricOperations {
    
    private Logger log = LoggerFactory.getLogger(QueryMetricOperations.class);
    
    private ShardTableQueryMetricHandler handler;
    private QueryGeometryHandler geometryHandler;
    private CacheManager cacheManager;
    private Cache incomingQueryMetricsCache;
    private Cache lastWrittenQueryMetricCache;
    private boolean isHazelCast;
    private MarkingFunctions markingFunctions;
    private TimelyProperties timelyProperties;
    private BaseQueryMetricListResponseFactory queryMetricListResponseFactory;
    private ReentrantLock caffeineLock = new ReentrantLock();
    private MergeLockLifecycleListener mergeLock;
    
    private final QueryMetricSupplier queryMetricSupplier;
    private final DnUtils dnUtils;
    
    /**
     * The enum Default datetime.
     */
    enum DEFAULT_DATETIME {
        /**
         * Begin default datetime.
         */
        BEGIN,
        /**
         * End default datetime.
         */
        END
    }
    
    /**
     * Instantiates a new QueryMetricOperations.
     *
     * @param cacheManager
     *            the CacheManager
     * @param handler
     *            the QueryMetricHandler
     * @param geometryHandler
     *            the QueryGeometryHandler
     * @param markingFunctions
     *            the MarkingFunctions
     * @param queryMetricListResponseFactory
     *            the QueryMetricListResponseFactory
     * @param timelyProperties
     *            the TimelyProperties
     * @param queryMetricSupplier
     *            the query metric object supplier
     * @param dnUtils
     *            the dnUtils
     */
    @Autowired
    public QueryMetricOperations(@Named("queryMetricCacheManager") CacheManager cacheManager, ShardTableQueryMetricHandler handler,
                    QueryGeometryHandler geometryHandler, MarkingFunctions markingFunctions, BaseQueryMetricListResponseFactory queryMetricListResponseFactory,
                    TimelyProperties timelyProperties, MergeLockLifecycleListener mergeLock, QueryMetricSupplier queryMetricSupplier, DnUtils dnUtils) {
        this.handler = handler;
        this.geometryHandler = geometryHandler;
        if (cacheManager instanceof HazelcastCacheManager) {
            this.isHazelCast = true;
        } else {
            this.isHazelCast = false;
        }
        this.cacheManager = cacheManager;
        this.incomingQueryMetricsCache = cacheManager.getCache(INCOMING_METRICS);
        this.lastWrittenQueryMetricCache = cacheManager.getCache(LAST_WRITTEN_METRICS);
        this.markingFunctions = markingFunctions;
        this.queryMetricListResponseFactory = queryMetricListResponseFactory;
        this.timelyProperties = timelyProperties;
        this.mergeLock = mergeLock;
        this.queryMetricSupplier = queryMetricSupplier;
        this.dnUtils = dnUtils;
    }
    
    /**
     * Update metrics void response.
     *
     * @param queryMetrics
     *            the list of query metric updates
     * @param metricType
     *            the metric type
     * @return the void response
     */
    // Messages that arrive via http/https get placed on the message queue
    // to ensure a quick response and to maintain a single queue of work
    @Operation(summary = "Submit a list of metrics updates.", description = "Metrics updates will be placed on a message queue to ensure a quick response.")
    @Secured({"Administrator", "JBossAdministrator", "MetricsAdministrator"})
    @RequestMapping(path = "/updateMetrics", method = {RequestMethod.POST}, consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public VoidResponse updateMetrics(@RequestBody List<BaseQueryMetric> queryMetrics,
                    @RequestParam(value = "metricType", defaultValue = "DISTRIBUTED") QueryMetricType metricType) {
        VoidResponse response = new VoidResponse();
        for (BaseQueryMetric m : queryMetrics) {
            if (log.isTraceEnabled()) {
                log.trace("received metric update via REST: " + m.toString());
            } else {
                log.debug("received metric update via REST: " + m.getQueryId());
            }
            queryMetricSupplier.send(MessageBuilder.withPayload(new QueryMetricUpdate(m, metricType)).build());
        }
        return response;
    }
    
    /**
     * Update metric void response.
     *
     * @param queryMetric
     *            the query metric update
     * @param metricType
     *            the metric type
     * @return the void response
     */
    // Messages that arrive via http/https get placed on the message queue
    // to ensure a quick response and to maintain a single queue of work
    @Operation(summary = "Submit a single metric update.", description = "The metric update will be placed on a message queue to ensure a quick response.")
    @Secured({"Administrator", "JBossAdministrator", "MetricsAdministrator"})
    @RequestMapping(path = "/updateMetric", method = {RequestMethod.POST}, consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public VoidResponse updateMetric(@RequestBody BaseQueryMetric queryMetric,
                    @RequestParam(value = "metricType", defaultValue = "DISTRIBUTED") QueryMetricType metricType) {
        if (log.isTraceEnabled()) {
            log.trace("received metric update via REST: " + queryMetric.toString());
        } else {
            log.debug("received metric update via REST: " + queryMetric.getQueryId());
        }
        queryMetricSupplier.send(MessageBuilder.withPayload(new QueryMetricUpdate(queryMetric, metricType)).build());
        return new VoidResponse();
    }
    
    private String getClusterLocalMemberUuid() {
        return ((HazelcastCacheManager) this.cacheManager).getHazelcastInstance().getCluster().getLocalMember().getUuid().toString();
    }
    
    /**
     * Store metric void response.
     *
     * @param queryMetric
     *            the query metric update
     * @param metricType
     *            the metric type
     * @return the void response
     */
    public VoidResponse storeMetric(BaseQueryMetric queryMetric, QueryMetricType metricType) {
        
        if (log.isTraceEnabled()) {
            log.trace("storing metric update: " + queryMetric.toString());
        } else {
            log.debug("storing metric update: " + queryMetric.getQueryId());
        }
        VoidResponse response = new VoidResponse();
        try {
            Long lastPageNum = null;
            String queryId = queryMetric.getQueryId();
            if (this.isHazelCast) {
                // use a native cache set vs Cache.put to prevent the fetching and return of accumulo value
                IMap<Object,Object> incomingQueryMetricsCacheHz = ((IMap<Object,Object>) incomingQueryMetricsCache.getNativeCache());
                
                // don't lock on the Hazelcast cluster when a merge is in progress
                this.mergeLock.lock();
                incomingQueryMetricsCacheHz.lock(queryId, 120, TimeUnit.SECONDS);
                try {
                    BaseQueryMetric updatedMetric = queryMetric;
                    QueryMetricUpdate lastQueryMetricUpdate = (QueryMetricUpdate) incomingQueryMetricsCacheHz.get(queryId);
                    if (lastQueryMetricUpdate == null) {
                        updatedMetric.setNumUpdates(1);
                    } else {
                        BaseQueryMetric lastQueryMetric = lastQueryMetricUpdate.getMetric();
                        updatedMetric.setNumUpdates(lastQueryMetric.getNumUpdates() + 1);
                        updatedMetric = handler.combineMetrics(updatedMetric, lastQueryMetric, metricType);
                        lastPageNum = getLastPageNumber(lastQueryMetric);
                    }
                    handler.populateMetricSelectors(updatedMetric);
                    incomingQueryMetricsCacheHz.set(queryId, new QueryMetricUpdate(updatedMetric, metricType));
                    sendMetricsToTimely(updatedMetric, lastPageNum);
                } finally {
                    try {
                        incomingQueryMetricsCacheHz.unlock(queryId);
                    } catch (Exception e) {
                        incomingQueryMetricsCacheHz.forceUnlock(queryId);
                    } finally {
                        this.mergeLock.unlock();
                    }
                }
            } else {
                caffeineLock.lock();
                try {
                    QueryMetricUpdate lastQueryMetricUpdate = incomingQueryMetricsCache.get(queryId, QueryMetricUpdate.class);
                    BaseQueryMetric updatedMetric = queryMetric;
                    if (lastQueryMetricUpdate == null) {
                        updatedMetric.setNumUpdates(1);
                    } else {
                        BaseQueryMetric lastQueryMetric = lastQueryMetricUpdate.getMetric();
                        updatedMetric.setNumUpdates(lastQueryMetric.getNumUpdates() + 1);
                        updatedMetric = handler.combineMetrics(updatedMetric, lastQueryMetric, metricType);
                        handler.writeMetric(updatedMetric, Collections.singletonList(lastQueryMetric), lastQueryMetric.getLastUpdated(), true);
                        lastPageNum = getLastPageNumber(lastQueryMetric);
                    }
                    handler.populateMetricSelectors(updatedMetric);
                    handler.writeMetric(updatedMetric, Collections.singletonList(updatedMetric), updatedMetric.getLastUpdated(), false);
                    this.incomingQueryMetricsCache.put(queryId, new QueryMetricUpdate(updatedMetric, metricType));
                    sendMetricsToTimely(updatedMetric, lastPageNum);
                } finally {
                    caffeineLock.unlock();
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(DatawaveErrorCode.UNKNOWN_SERVER_ERROR, e));
        }
        return response;
    }
    
    private Long getLastPageNumber(BaseQueryMetric m) {
        Long lastPage = null;
        List<BaseQueryMetric.PageMetric> pageMetrics = m.getPageTimes();
        for (BaseQueryMetric.PageMetric pm : pageMetrics) {
            if (lastPage == null || pm.getPageNumber() > lastPage) {
                lastPage = pm.getPageNumber();
            }
        }
        return lastPage;
    }
    
    /**
     * Returns metrics for the current users queries that are identified by the id
     *
     * @param currentUser
     *            the current user
     * @param queryId
     *            the query id
     * @return the base query metric list response
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Get the metrics for a given query ID.")
    @PermitAll
    @RequestMapping(path = "/id/{queryId}", method = {RequestMethod.GET},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_HTML_VALUE})
    public BaseQueryMetricListResponse query(@AuthenticationPrincipal ProxiedUserDetails currentUser,
                    @Parameter(description = "queryId to return") @PathVariable("queryId") String queryId) {
        
        BaseQueryMetricListResponse response = this.queryMetricListResponseFactory.createDetailedResponse();
        List<BaseQueryMetric> metricList = new ArrayList<>();
        try {
            QueryMetricUpdate metricUpdate = incomingQueryMetricsCache.get(queryId, QueryMetricUpdate.class);
            if (metricUpdate != null) {
                BaseQueryMetric metric = metricUpdate.getMetric();
                boolean allowAllMetrics = false;
                boolean sameUser = false;
                if (currentUser != null) {
                    String metricUser = metric.getUser();
                    String requestingUser = dnUtils.getShortName(currentUser.getPrimaryUser().getName());
                    sameUser = metricUser != null && metricUser.equals(requestingUser);
                    allowAllMetrics = currentUser.getPrimaryUser().getRoles().contains("MetricsAdministrator");
                }
                // since we are using the incomingQueryMetricsCache, we need to make sure that the
                // requesting user has the necessary Authorizations to view the requested query metric
                if (sameUser || allowAllMetrics) {
                    ColumnVisibility columnVisibility = this.markingFunctions.translateToColumnVisibility(metric.getMarkings());
                    boolean userCanSeeVisibility = true;
                    for (DatawaveUser user : currentUser.getProxiedUsers()) {
                        Authorizations authorizations = new Authorizations(user.getAuths().toArray(new String[0]));
                        VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(authorizations);
                        if (visibilityEvaluator.evaluate(columnVisibility) == false) {
                            userCanSeeVisibility = false;
                            break;
                        }
                    }
                    if (userCanSeeVisibility) {
                        metricList.add(metric);
                    }
                }
            }
        } catch (Exception e) {
            response.addException(new QueryException(e.getMessage(), 500));
        }
        // Set the result to have the formatted query and query plan
        response.setResult(JexlFormattedStringBuildingVisitor.formatMetrics(metricList));
        if (metricList.isEmpty()) {
            response.setHasResults(false);
        } else {
            response.setGeoQuery(metricList.stream().anyMatch(SimpleQueryGeometryHandler::isGeoQuery));
            response.setHasResults(true);
        }
        return response;
    }
    
    /**
     * Get a map for the given query represented by the query ID, if applicable.
     *
     * @param currentUser
     *            the current user
     * @param queryId
     *            the query id
     * @return the query geometry response
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Get a map for the given query represented by the query ID, if applicable.")
    @PermitAll
    @RequestMapping(path = "/id/{queryId}/map", method = {RequestMethod.GET, RequestMethod.POST},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_HTML_VALUE})
    public QueryGeometryResponse map(@AuthenticationPrincipal ProxiedUserDetails currentUser, @PathVariable("queryId") String queryId) {
        QueryGeometryResponse queryGeometryResponse = new QueryGeometryResponse();
        BaseQueryMetricListResponse metricResponse = query(currentUser, queryId);
        if (!metricResponse.getExceptions().isEmpty()) {
            metricResponse.getExceptions().forEach(e -> queryGeometryResponse.addException(new QueryException(e.getMessage(), e.getCause(), e.getCode())));
            return queryGeometryResponse;
        } else {
            return geometryHandler.getQueryGeometryResponse(queryId, metricResponse.getResult());
        }
    }
    
    private static Date parseDate(String dateString, DEFAULT_DATETIME defaultDateTime) throws IllegalArgumentException {
        if (StringUtils.isBlank(dateString)) {
            return null;
        } else {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
                sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
                dateString = dateString.trim();
                if (dateString.length() == 8) {
                    dateString = dateString + (defaultDateTime.equals(BEGIN) ? " 000000.000" : " 235959.999");
                } else if (dateString.length() == 15) {
                    dateString = dateString + (defaultDateTime.equals(BEGIN) ? ".000" : ".999");
                }
                return sdf.parse(dateString);
            } catch (ParseException e) {
                String parameter = defaultDateTime.equals(BEGIN) ? "begin" : "end";
                throw new IllegalArgumentException(
                                "Unable to parse parameter '" + parameter + "' - valid formats: yyyyMMdd | yyyyMMdd HHmmss | yyyyMMdd HHmmss.SSS");
            }
        }
    }
    
    private QueryMetricsSummaryResponse queryMetricsSummary(Date begin, Date end, ProxiedUserDetails currentUser, boolean onlyCurrentUser) {
        
        if (null == end) {
            end = new Date();
        }
        Calendar ninetyDaysBeforeEnd = Calendar.getInstance();
        ninetyDaysBeforeEnd.setTime(end);
        ninetyDaysBeforeEnd.add(Calendar.DATE, -90);
        if (null == begin) {
            // ninety days before end
            begin = ninetyDaysBeforeEnd.getTime();
        }
        QueryMetricsSummaryResponse response;
        if (end.before(begin)) {
            response = new QueryMetricsSummaryResponse();
            String s = "begin date can not be after end date";
            response.addException(new QueryException(DatawaveErrorCode.BEGIN_DATE_AFTER_END_DATE, new IllegalArgumentException(s), s));
        } else {
            response = handler.getQueryMetricsSummary(begin, end, currentUser, onlyCurrentUser);
        }
        return response;
    }
    
    /**
     * Returns a summary of the query metrics
     *
     * @param currentUser
     *            the current user
     * @param begin
     *            formatted date/time (yyyyMMdd | yyyyMMdd HHmmss | yyyyMMdd HHmmss.SSS)
     * @param end
     *            formatted date/time (yyyyMMdd | yyyyMMdd HHmmss | yyyyMMdd HHmmss.SSS)
     * @return the query metrics summary
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Get a summary of the query metrics.")
    @Secured({"Administrator", "JBossAdministrator", "MetricsAdministrator"})
    @RequestMapping(path = "/summary/all", method = {RequestMethod.GET},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_HTML_VALUE})
    public QueryMetricsSummaryResponse getQueryMetricsSummary(@AuthenticationPrincipal ProxiedUserDetails currentUser,
                    @RequestParam(required = false) String begin, @RequestParam(required = false) String end) {
        
        try {
            return queryMetricsSummary(parseDate(begin, BEGIN), parseDate(end, END), currentUser, false);
        } catch (Exception e) {
            QueryMetricsSummaryResponse response = new QueryMetricsSummaryResponse();
            response.addException(e);
            return response;
        }
    }
    
    /**
     * Returns a summary of the requesting user's query metrics
     *
     * @param currentUser
     *            the current user
     * @param begin
     *            formatted date/time (yyyyMMdd | yyyyMMdd HHmmss | yyyyMMdd HHmmss.SSS)
     * @param end
     *            formatted date/time (yyyyMMdd | yyyyMMdd HHmmss | yyyyMMdd HHmmss.SSS)
     * @return the query metrics user summary
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Get a summary of the query metrics for the given user.")
    @PermitAll
    @RequestMapping(path = "/summary/user", method = {RequestMethod.GET},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_HTML_VALUE})
    public QueryMetricsSummaryResponse getQueryMetricsUserSummary(@AuthenticationPrincipal ProxiedUserDetails currentUser,
                    @RequestParam(required = false) String begin, @RequestParam(required = false) String end) {
        try {
            return queryMetricsSummary(parseDate(begin, BEGIN), parseDate(end, END), currentUser, true);
        } catch (Exception e) {
            QueryMetricsSummaryResponse response = new QueryMetricsSummaryResponse();
            response.addException(e);
            return response;
        }
    }
    
    private UdpClient createUdpClient() {
        if (timelyProperties != null && StringUtils.isNotBlank(timelyProperties.getHost())) {
            return new UdpClient(timelyProperties.getHost(), timelyProperties.getPort());
        } else {
            return null;
        }
    }
    
    private void sendMetricsToTimely(BaseQueryMetric queryMetric, Long lastPageNum) {
        
        if (this.timelyProperties.getEnabled()) {
            UdpClient timelyClient = createUdpClient();
            if (queryMetric.getQueryType().equalsIgnoreCase("RunningQuery")) {
                try {
                    Lifecycle lifecycle = queryMetric.getLifecycle();
                    Map<String,String> metricValues = handler.getEventFields(queryMetric);
                    long createDate = queryMetric.getCreateDate().getTime();
                    
                    StringBuilder tagSb = new StringBuilder();
                    List<String> configuredMetricTags = timelyProperties.getTags();
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
                        List<BaseQueryMetric.PageMetric> pageTimes = queryMetric.getPageTimes();
                        // there should only be a maximum of one page metric as all but the last are removed by the QueryMetricsBean
                        for (BaseQueryMetric.PageMetric pm : pageTimes) {
                            // prevent duplicate reporting
                            if (lastPageNum == null || pm.getPageNumber() > lastPageNum) {
                                long requestTime = pm.getPageRequested();
                                long callTime = pm.getCallTime();
                                if (callTime == -1) {
                                    callTime = pm.getReturnTime();
                                }
                                if (pm.getPagesize() > 0) {
                                    timelyClient.write("put dw.query.metrics.PAGE_METRIC.calltime " + requestTime + " " + callTime + " " + tagSb);
                                    DecimalFormat df = new DecimalFormat("0.00");
                                    String callTimePerRecord = df.format((double) callTime / pm.getPagesize());
                                    timelyClient.write("put dw.query.metrics.PAGE_METRIC.calltimeperrecord " + requestTime + " " + callTimePerRecord + " "
                                                    + tagSb);
                                }
                            }
                        }
                    }
                    
                    if (lifecycle.equals(Lifecycle.CLOSED) || lifecycle.equals(Lifecycle.CANCELLED)) {
                        // write ELAPSED_TIME
                        timelyClient.write("put dw.query.metrics.ELAPSED_TIME " + createDate + " " + queryMetric.getElapsedTime() + " " + tagSb);
                        
                        // write NUM_RESULTS
                        timelyClient.write("put dw.query.metrics.NUM_RESULTS " + createDate + " " + queryMetric.getNumResults() + " " + tagSb);
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
    
    /**
     * Returns cache stats for the local part of the distributed Hazelcast cache
     *
     * @return the cache stats
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Get the query metrics cache stats.")
    @Secured({"Administrator", "JBossAdministrator", "MetricsAdministrator"})
    @RequestMapping(path = "/cacheStats", method = {RequestMethod.GET}, produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public CacheStats getCacheStats() {
        CacheStats cacheStats = new CacheStats();
        if (this.isHazelCast) {
            IMap<Object,Object> incomingCacheHz = ((IMap<Object,Object>) incomingQueryMetricsCache.getNativeCache());
            cacheStats.setIncomingQueryMetrics(getStats(incomingCacheHz.getLocalMapStats()));
            IMap<Object,Object> lastWrittenCacheHz = ((IMap<Object,Object>) lastWrittenQueryMetricCache.getNativeCache());
            cacheStats.setLastWrittenQueryMetrics(getStats(lastWrittenCacheHz.getLocalMapStats()));
            cacheStats.setMemberUuid(getClusterLocalMemberUuid());
            try {
                cacheStats.setHost(InetAddress.getLocalHost().getCanonicalHostName());
            } catch (Exception e) {
                
            }
        }
        return cacheStats;
    }
    
    private Map<String,Long> getStats(LocalMapStats localMapStats) {
        Map<String,Long> stats = new LinkedHashMap<>();
        stats.put("getCount", localMapStats.getGetOperationCount());
        stats.put("putCount", localMapStats.getPutOperationCount());
        stats.put("removeCount", localMapStats.getRemoveOperationCount());
        stats.put("numberOfOtherOperations", localMapStats.getOtherOperationCount());
        stats.put("numberOfEvents", localMapStats.getEventOperationCount());
        stats.put("lastAccessTime", localMapStats.getLastAccessTime());
        stats.put("lastUpdateTime", localMapStats.getLastUpdateTime());
        stats.put("hits", localMapStats.getHits());
        stats.put("ownedEntryCount", localMapStats.getOwnedEntryCount());
        stats.put("backupEntryCount", localMapStats.getBackupEntryCount());
        stats.put("backupCount", Long.valueOf(localMapStats.getBackupCount()));
        stats.put("ownedEntryMemoryCost", localMapStats.getOwnedEntryMemoryCost());
        stats.put("backupEntryMemoryCost", localMapStats.getBackupEntryMemoryCost());
        stats.put("creationTime", localMapStats.getCreationTime());
        stats.put("lockedEntryCount", localMapStats.getLockedEntryCount());
        stats.put("dirtyEntryCount", localMapStats.getDirtyEntryCount());
        
        // keep the contract as milliseconds for latencies sent using Json
        stats.put("totalGetLatencies", NANOSECONDS.toMillis(localMapStats.getTotalGetLatency()));
        stats.put("totalPutLatencies", NANOSECONDS.toMillis(localMapStats.getTotalPutLatency()));
        stats.put("totalRemoveLatencies", NANOSECONDS.toMillis(localMapStats.getTotalRemoveLatency()));
        stats.put("maxGetLatency", NANOSECONDS.toMillis(localMapStats.getMaxGetLatency()));
        stats.put("maxPutLatency", NANOSECONDS.toMillis(localMapStats.getMaxPutLatency()));
        stats.put("maxRemoveLatency", NANOSECONDS.toMillis(localMapStats.getMaxRemoveLatency()));
        
        stats.put("heapCost", localMapStats.getHeapCost());
        stats.put("merkleTreesCost", localMapStats.getMerkleTreesCost());
        stats.put("queryCount", localMapStats.getQueryCount());
        stats.put("indexedQueryCount", localMapStats.getIndexedQueryCount());
        
        return stats;
    }
}
