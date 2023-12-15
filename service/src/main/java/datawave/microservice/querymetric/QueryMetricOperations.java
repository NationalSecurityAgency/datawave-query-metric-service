package datawave.microservice.querymetric;

import com.codahale.metrics.Timer;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IMap;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.marking.MarkingFunctions;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.querymetric.config.QueryMetricProperties;
import datawave.microservice.querymetric.config.QueryMetricProperties.Retry;
import datawave.microservice.querymetric.config.QueryMetricSinkConfiguration.QueryMetricSinkBinding;
import datawave.microservice.querymetric.factory.BaseQueryMetricListResponseFactory;
import datawave.microservice.querymetric.handler.QueryGeometryHandler;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.handler.SimpleQueryGeometryHandler;
import datawave.security.authorization.DatawaveUser;
import datawave.security.util.DnUtils;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;
import io.swagger.annotations.ApiParam;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.http.MediaType;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PreDestroy;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Named;
import java.net.InetAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

import static datawave.microservice.querymetric.QueryMetricOperations.DEFAULT_DATETIME.BEGIN;
import static datawave.microservice.querymetric.QueryMetricOperations.DEFAULT_DATETIME.END;
import static datawave.microservice.querymetric.config.HazelcastMetricCacheConfiguration.INCOMING_METRICS;
import static datawave.microservice.querymetric.config.QueryMetricSourceConfiguration.QueryMetricSourceBinding.SOURCE_NAME;
import static datawave.microservice.querymetric.QueryMetricOperationsStats.METERS;
import static datawave.microservice.querymetric.QueryMetricOperationsStats.TIMERS;

/**
 * The type Query metric operations.
 */
@EnableBinding(QueryMetricSinkBinding.class)
@EnableScheduling
@RestController
@RequestMapping(path = "/v1")
public class QueryMetricOperations {
    
    private Logger log = LoggerFactory.getLogger(QueryMetricOperations.class);
    
    private QueryMetricProperties queryMetricProperties;
    private ShardTableQueryMetricHandler handler;
    private QueryGeometryHandler geometryHandler;
    private CacheManager cacheManager;
    private Cache incomingQueryMetricsCache;
    private MarkingFunctions markingFunctions;
    private BaseQueryMetricListResponseFactory queryMetricListResponseFactory;
    private MergeLockLifecycleListener mergeLock;
    private Correlator correlator;
    private AtomicBoolean timedCorrelationInProgress = new AtomicBoolean(false);
    private MetricUpdateEntryProcessorFactory entryProcessorFactory;
    private QueryMetricOperationsStats stats;
    private static Set<String> inProcess = Collections.synchronizedSet(new HashSet<>());
    private final LinkedHashMap<String,String> pathPrefixMap = new LinkedHashMap<>();
    
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
    
    @Output(SOURCE_NAME)
    @Autowired
    private MessageChannel output;
    
    /**
     * Instantiates a new QueryMetricOperations.
     *
     * @param queryMetricProperties
     *            the query metric properties
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
     * @param mergeLock
     *            the merge lock
     * @param entryProcessorFactory
     *            the entry processor factory
     * @param stats
     *            the stats
     */
    @Autowired
    public QueryMetricOperations(QueryMetricProperties queryMetricProperties, @Named("queryMetricCacheManager") CacheManager cacheManager,
                    ShardTableQueryMetricHandler handler, QueryGeometryHandler geometryHandler, MarkingFunctions markingFunctions,
                    BaseQueryMetricListResponseFactory queryMetricListResponseFactory, MergeLockLifecycleListener mergeLock, Correlator correlator,
                    MetricUpdateEntryProcessorFactory entryProcessorFactory, QueryMetricOperationsStats stats) {
        this.queryMetricProperties = queryMetricProperties;
        this.handler = handler;
        this.geometryHandler = geometryHandler;
        this.cacheManager = cacheManager;
        this.incomingQueryMetricsCache = cacheManager.getCache(INCOMING_METRICS);
        this.markingFunctions = markingFunctions;
        this.queryMetricListResponseFactory = queryMetricListResponseFactory;
        this.mergeLock = mergeLock;
        this.correlator = correlator;
        this.entryProcessorFactory = entryProcessorFactory;
        this.stats = stats;
        this.pathPrefixMap.put("jquery", "/querymetric/webjars/jquery");
        this.pathPrefixMap.put("leaflet", "/querymetric/webjars/leaflet");
        this.pathPrefixMap.put("css", "/querymetric/css");
        this.pathPrefixMap.put("js", "/querymetric/js");
    }
    
    @PreDestroy
    public void shutdown() {
        if (this.correlator.isEnabled()) {
            this.correlator.shutdown(true);
            // we've locked out the timer thread, but need to
            // wait for it to complete the last write
            while (isTimedCorrelationInProgress()) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    
                }
            }
            ensureUpdatesProcessed(false);
        }
        this.stats.queueAggregatedQueryStatsForTimely();
        this.stats.writeQueryStatsToTimely();
    }
    
    public boolean isTimedCorrelationInProgress() {
        return this.timedCorrelationInProgress.get();
    }
    
    @Scheduled(fixedDelay = 2000)
    public void ensureUpdatesProcessedScheduled() {
        // don't write metrics from this thread while shutting down,
        // so we can make sure that the process completes
        if (!this.correlator.isShuttingDown()) {
            this.timedCorrelationInProgress.set(true);
            try {
                ensureUpdatesProcessed(true);
            } finally {
                this.timedCorrelationInProgress.set(false);
            }
        }
    }
    
    public void ensureUpdatesProcessed(boolean scheduled) {
        if (this.correlator.isEnabled()) {
            List<QueryMetricUpdate> correlatedUpdates;
            do {
                correlatedUpdates = this.correlator.getMetricUpdates(QueryMetricOperations.inProcess);
                if (correlatedUpdates != null && !correlatedUpdates.isEmpty()) {
                    try {
                        String queryId = correlatedUpdates.get(0).getMetric().getQueryId();
                        QueryMetricType metricType = correlatedUpdates.get(0).getMetricType();
                        QueryMetricUpdateHolder metricUpdate = combineMetricUpdates(correlatedUpdates, metricType);
                        log.debug("storing correlated updates for {}", queryId);
                        storeMetricUpdate(metricUpdate);
                    } catch (Exception e) {
                        log.error("exception while combining correlated updates: " + e.getMessage(), e);
                    }
                }
            } while (!(scheduled && this.correlator.isShuttingDown()) && correlatedUpdates != null && !correlatedUpdates.isEmpty());
        }
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
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/updateMetrics", method = {RequestMethod.POST}, consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public VoidResponse updateMetrics(@RequestBody List<BaseQueryMetric> queryMetrics,
                    @RequestParam(value = "metricType", defaultValue = "DISTRIBUTED") QueryMetricType metricType) {
        if (!this.mergeLock.isAllowedReadLock()) {
            throw new IllegalStateException("service unavailable");
        }
        Timer.Context restTimer = this.stats.getTimer(TIMERS.REST).time();
        try {
            for (BaseQueryMetric m : queryMetrics) {
                if (log.isTraceEnabled()) {
                    log.trace("received metric update via REST: " + m.toString());
                } else {
                    log.debug("received metric update via REST: " + m.getQueryId());
                }
                Timer.Context messageSendTimer = this.stats.getTimer(TIMERS.MESSAGE_SEND).time();
                try {
                    if (!updateMetric(new QueryMetricUpdate<>(m, metricType))) {
                        throw new RuntimeException("Unable to process query metric update for query [" + m.getQueryId() + "]");
                    }
                } finally {
                    messageSendTimer.stop();
                }
            }
        } finally {
            restTimer.stop();
        }
        return new VoidResponse();
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
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/updateMetric", method = {RequestMethod.POST}, consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public VoidResponse updateMetric(@RequestBody BaseQueryMetric queryMetric,
                    @RequestParam(value = "metricType", defaultValue = "DISTRIBUTED") QueryMetricType metricType) {
        if (!this.mergeLock.isAllowedReadLock()) {
            throw new IllegalStateException("service unavailable");
        }
        Timer.Context restTimer = this.stats.getTimer(TIMERS.REST).time();
        try {
            if (log.isTraceEnabled()) {
                log.trace("received metric update via REST: " + queryMetric.toString());
            } else {
                log.debug("received metric update via REST: " + queryMetric.getQueryId());
            }
            Timer.Context messageSendTimer = this.stats.getTimer(TIMERS.MESSAGE_SEND).time();
            try {
                if (!updateMetric(new QueryMetricUpdate(queryMetric, metricType))) {
                    throw new RuntimeException("Unable to process query metric update for query [" + queryMetric.getQueryId() + "]");
                }
            } finally {
                messageSendTimer.stop();
            }
        } finally {
            restTimer.stop();
        }
        return new VoidResponse();
    }
    
    private boolean updateMetric(QueryMetricUpdate update) {
        
        boolean success;
        final long updateStartTime = System.currentTimeMillis();
        long currentTime;
        int attempts = 0;
        
        Retry retry = queryMetricProperties.getRetry();
        
        do {
            if (attempts++ > 0) {
                try {
                    Thread.sleep(retry.getBackoffIntervalMillis());
                } catch (InterruptedException e) {
                    // Ignore -- we'll just end up retrying a little too fast
                }
            }
            
            if (log.isDebugEnabled()) {
                log.debug("Update attempt {} of {} for query {}", attempts, retry.getMaxAttempts(), update.getMetric().getQueryId());
            }
            
            success = output.send(MessageBuilder.withPayload(update).build());
            currentTime = System.currentTimeMillis();
        } while (!success && (currentTime - updateStartTime) < retry.getFailTimeoutMillis() && attempts < retry.getMaxAttempts());
        
        if (!success) {
            log.warn("Update for query {} failed. {attempts = {}, elapsedMillis = {}}", update.getMetric().getQueryId(), attempts,
                            (currentTime - updateStartTime));
        } else {
            log.info("Update for query {} successful. {attempts = {}, elapsedMillis = {}}", update.getMetric().getQueryId(), attempts,
                            (currentTime - updateStartTime));
        }
        
        return success;
    }
    
    /**
     * Passes query metric messages to the messaging infrastructure.
     * <p>
     * The metric ID is used as a correlation ID in order to ensure that a producer confirm ack is received. If a producer confirm ack is not received within
     * the specified amount of time, a 500 Internal Server Error will be returned to the caller.
     *
     * @param update
     *            The query metric update to be sent
     */
    
    private boolean shouldCorrelate(QueryMetricUpdate update) {
        // add the first update for a metric to get it into the cache
        if ((update.getMetric().getLifecycle().ordinal() <= BaseQueryMetric.Lifecycle.DEFINED.ordinal())) {
            return false;
        }
        if (this.correlator.isEnabled()) {
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * Handle event.
     *
     * @param update
     *            the query metric update
     */
    @StreamListener(QueryMetricSinkBinding.SINK_NAME)
    public void handleEvent(QueryMetricUpdate update) {
        this.stats.queueTimelyMetricUpdate(update);
        this.stats.getMeter(METERS.MESSAGE_RECEIVE).mark();
        if (shouldCorrelate(update)) {
            log.debug("adding update for {} to correlator", update.getMetric().getQueryId());
            this.correlator.addMetricUpdate(update);
        } else {
            log.debug("storing update for {}", update.getMetric().getQueryId());
            storeMetricUpdate(new QueryMetricUpdateHolder(update));
        }
        
        if (this.correlator.isEnabled()) {
            List<QueryMetricUpdate> correlatedUpdates;
            do {
                correlatedUpdates = this.correlator.getMetricUpdates(QueryMetricOperations.inProcess);
                if (correlatedUpdates != null && !correlatedUpdates.isEmpty()) {
                    try {
                        String queryId = correlatedUpdates.get(0).getMetric().getQueryId();
                        QueryMetricType metricType = correlatedUpdates.get(0).getMetricType();
                        QueryMetricUpdateHolder metricUpdate = combineMetricUpdates(correlatedUpdates, metricType);
                        log.debug("storing correlated updates for {}", queryId);
                        storeMetricUpdate(metricUpdate);
                    } catch (Exception e) {
                        log.error("exception while combining correlated updates: " + e.getMessage(), e);
                    }
                }
            } while (correlatedUpdates != null && !correlatedUpdates.isEmpty());
        }
    }
    
    private String getClusterLocalMemberUuid() {
        return ((HazelcastCacheManager) this.cacheManager).getHazelcastInstance().getCluster().getLocalMember().getUuid();
    }
    
    private QueryMetricUpdateHolder combineMetricUpdates(List<QueryMetricUpdate> updates, QueryMetricType metricType) throws Exception {
        BaseQueryMetric combinedMetric = null;
        BaseQueryMetric.Lifecycle lowestLifecycle = null;
        for (QueryMetricUpdate u : updates) {
            if (combinedMetric == null) {
                combinedMetric = u.getMetric();
                lowestLifecycle = u.getMetric().getLifecycle();
            } else {
                if (u.getMetric().getLifecycle().ordinal() < lowestLifecycle.ordinal()) {
                    lowestLifecycle = u.getMetric().getLifecycle();
                }
                combinedMetric = this.handler.combineMetrics(u.getMetric(), combinedMetric, metricType);
            }
        }
        QueryMetricUpdateHolder metricUpdateHolder = new QueryMetricUpdateHolder(combinedMetric, metricType);
        metricUpdateHolder.updateLowestLifecycle(lowestLifecycle);
        return metricUpdateHolder;
    }
    
    private void storeMetricUpdate(QueryMetricUpdateHolder metricUpdate) {
        Timer.Context storeTimer = this.stats.getTimer(TIMERS.STORE).time();
        String queryId = metricUpdate.getMetric().getQueryId();
        try {
            IMap<Object,Object> incomingQueryMetricsCacheHz = ((IMap<Object,Object>) incomingQueryMetricsCache.getNativeCache());
            if (metricUpdate.getMetric().getPositiveSelectors() == null) {
                this.handler.populateMetricSelectors(metricUpdate.getMetric());
            }
            this.mergeLock.lock();
            QueryMetricOperations.inProcess.add(queryId);
            try {
                incomingQueryMetricsCacheHz.executeOnKey(queryId, this.entryProcessorFactory.createEntryProcessor(metricUpdate));
            } finally {
                QueryMetricOperations.inProcess.remove(queryId);
                this.mergeLock.unlock();
            }
        } catch (Exception e) {
            if (!this.mergeLock.isShuttingDown()) {
                if (e instanceof HazelcastInstanceNotActiveException) {
                    log.error("HazelcastInstanceNotActiveException - OK if shutting down");
                } else {
                    log.error(e.getMessage(), e);
                }
            }
            // fail the handling of the message
            throw new RuntimeException(e.getMessage());
        } finally {
            storeTimer.stop();
        }
    }
    
    /**
     * Returns metrics for the current users queries that are identified by the id
     *
     * @param currentUser
     *            the current user
     * @param queryId
     *            the query id
     * @return datawave.webservice.result.QueryMetricListResponse base query metric list response
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @PermitAll
    @RequestMapping(path = "/id/{queryId}", method = {RequestMethod.GET},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_HTML_VALUE})
    public BaseQueryMetricListResponse query(@AuthenticationPrincipal ProxiedUserDetails currentUser,
                    @ApiParam("queryId to return") @PathVariable("queryId") String queryId) {
        
        BaseQueryMetricListResponse response = this.queryMetricListResponseFactory.createDetailedResponse();
        response.setHtmlIncludePaths(this.pathPrefixMap);
        response.setBaseUrl("/querymetric/v1");
        List<BaseQueryMetric> metricList = new ArrayList<>();
        try {
            BaseQueryMetric metric;
            QueryMetricUpdateHolder metricUpdate = incomingQueryMetricsCache.get(queryId, QueryMetricUpdateHolder.class);
            if (metricUpdate == null) {
                metric = this.handler.getQueryMetric(queryId);
            } else {
                metric = metricUpdate.getMetric();
            }
            if (metric != null) {
                boolean allowAllMetrics = false;
                boolean sameUser = false;
                if (currentUser != null) {
                    String metricUser = metric.getUser();
                    String requestingUser = DnUtils.getShortName(currentUser.getPrimaryUser().getName());
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
        response.setResult(metricList);
        if (metricList.isEmpty()) {
            response.setHasResults(false);
        } else {
            response.setGeoQuery(metricList.stream().anyMatch(SimpleQueryGeometryHandler::isGeoQuery));
            response.setHasResults(true);
        }
        return response;
    }
    
    /**
     * Returns metrics for the current users queries that are identified by the id
     *
     * @param currentUser
     *            the current user
     * @param queryId
     *            the query id
     * @return datawave.webservice.result.QueryMetricListResponse query geometry response
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @PermitAll
    @RequestMapping(path = "/id/{queryId}/map", method = {RequestMethod.GET, RequestMethod.POST},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_HTML_VALUE})
    public QueryGeometryResponse map(@AuthenticationPrincipal ProxiedUserDetails currentUser, @PathVariable("queryId") String queryId) {
        QueryGeometryResponse queryGeometryResponse = new QueryGeometryResponse();
        BaseQueryMetricListResponse metricResponse = query(currentUser, queryId);
        if (metricResponse.getExceptions() == null || metricResponse.getExceptions().isEmpty()) {
            QueryGeometryResponse response = geometryHandler.getQueryGeometryResponse(queryId, metricResponse.getResult());
            response.setHtmlIncludePaths(pathPrefixMap);
            return response;
        } else {
            metricResponse.getExceptions().forEach(e -> queryGeometryResponse.addException(new QueryException(e.getMessage(), e.getCause(), e.getCode())));
            return queryGeometryResponse;
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
     * @return datawave.microservice.querymetric.QueryMetricsSummaryResponse query metrics summary
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @RolesAllowed({"Administrator", "JBossAdministrator", "MetricsAdministrator"})
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
     * @return datawave.microservice.querymetric.QueryMetricsSummaryResponse query metrics user summary
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
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
    
    /**
     * Returns cache stats for the local part of the distributed Hazelcast cache
     *
     * @return datawave.microservice.querymetric.CacheStats
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @RolesAllowed({"Administrator", "JBossAdministrator", "MetricsAdministrator"})
    @RequestMapping(path = "/stats", method = {RequestMethod.GET}, produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public CacheStats getCacheStats() {
        CacheStats cacheStats = new CacheStats();
        IMap<Object,Object> incomingCacheHz = ((IMap<Object,Object>) incomingQueryMetricsCache.getNativeCache());
        cacheStats.setIncomingQueryMetrics(this.stats.getLocalMapStats(incomingCacheHz.getLocalMapStats()));
        cacheStats.setServiceStats(this.stats.formatStats(this.stats.getServiceStats(), true));
        cacheStats.setMemberUuid(getClusterLocalMemberUuid());
        try {
            cacheStats.setHost(InetAddress.getLocalHost().getCanonicalHostName());
        } catch (Exception e) {
            
        }
        return cacheStats;
    }
    
    @Scheduled(fixedRateString = "${datawave.query.metric.stats.logServiceStatsRateMs:300000}")
    public void logStats() {
        this.stats.logServiceStats();
    }
    
    @Scheduled(fixedRateString = "${datawave.query.metric.stats.publishServiceStatsToTimelyRateMs:60000}")
    public void publishServiceStatsToTimely() {
        this.stats.writeServiceStatsToTimely();
    }
    
    @Scheduled(fixedRateString = "${datawave.query.metric.stats.publishQueryStatsToTimelyRateMs:60000}")
    public void publishQueryStatsToTimely() {
        this.stats.queueAggregatedQueryStatsForTimely();
        this.stats.writeQueryStatsToTimely();
    }
}
