package datawave.microservice.querymetrics;

import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.querymetrics.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetrics.handler.ShardTableQueryMetricHandler;
import datawave.security.util.DnUtils;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.metric.BaseQueryMetricListResponse;
import datawave.webservice.query.metric.QueryMetricListResponse;
import datawave.webservice.result.VoidResponse;
import io.swagger.annotations.ApiParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import static datawave.microservice.querymetrics.config.HazelcastServerConfiguration.INCOMING_METRICS;
import static datawave.microservice.querymetrics.config.HazelcastServerConfiguration.LAST_WRITTEN_METRICS;

@RestController
@RequestMapping(path = "/v1")
public class QueryMetricOperations {
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    private ShardTableQueryMetricHandler handler;
    private Cache incomingQueryMetricsCache;
    private Cache lastWrittenQueryMetricCache;
    private boolean isHazelCast;
    QueryMetricHandlerProperties queryMetricHandlerProperties;
    private ReentrantLock caffeineLock = new ReentrantLock();
    
    @Autowired
    public QueryMetricOperations(CacheManager cacheManager, ShardTableQueryMetricHandler handler, QueryMetricHandlerProperties queryMetricHandlerProperties) {
        this.handler = handler;
        this.isHazelCast = cacheManager instanceof HazelcastCacheManager;
        this.incomingQueryMetricsCache = cacheManager.getCache(INCOMING_METRICS);
        this.lastWrittenQueryMetricCache = cacheManager.getCache(LAST_WRITTEN_METRICS);
        this.queryMetricHandlerProperties = queryMetricHandlerProperties;
    }
    
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/updateMetrics", method = {RequestMethod.POST}, consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public VoidResponse update(@RequestBody List<BaseQueryMetric> queryMetrics) {
        VoidResponse response = new VoidResponse();
        for (BaseQueryMetric m : queryMetrics) {
            response = update(m);
            List<QueryExceptionType> exceptions = response.getExceptions();
            if (exceptions != null && !exceptions.isEmpty()) {
                break;
            }
        }
        return response;
    }
    
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/updateMetric", method = {RequestMethod.POST}, consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public VoidResponse update(@RequestBody BaseQueryMetric queryMetric) {
        
        VoidResponse response = new VoidResponse();
        try {
            String queryId = queryMetric.getQueryId();
            if (this.isHazelCast) {
                // use a native cache set vs Cache.put to prevent the fetching and return of accumulo value
                MapProxyImpl incomingQueryMetricsCacheHz = ((MapProxyImpl) incomingQueryMetricsCache.getNativeCache());
                
                incomingQueryMetricsCacheHz.lock(queryId);
                try {
                    BaseQueryMetric metricToCache = queryMetric;
                    BaseQueryMetric lastQueryMetric = (BaseQueryMetric) incomingQueryMetricsCacheHz.get(queryId);
                    if (lastQueryMetric != null) {
                        metricToCache = handler.combineMetrics(queryMetric, lastQueryMetric);
                    }
                    incomingQueryMetricsCacheHz.set(queryId, metricToCache);
                } finally {
                    incomingQueryMetricsCacheHz.unlock(queryId);
                }
            } else {
                caffeineLock.lock();
                try {
                    BaseQueryMetric lastQueryMetric = incomingQueryMetricsCache.get(queryId, BaseQueryMetric.class);
                    BaseQueryMetric updatedMetric = queryMetric;
                    if (lastQueryMetric != null) {
                        updatedMetric = handler.combineMetrics(queryMetric, lastQueryMetric);
                        handler.writeMetric(updatedMetric, Collections.singletonList(lastQueryMetric), lastQueryMetric.getLastUpdated(), true);
                    }
                    handler.writeMetric(updatedMetric, Collections.singletonList(updatedMetric), updatedMetric.getLastUpdated(), false);
                    this.incomingQueryMetricsCache.put(queryId, updatedMetric);
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
    
    /**
     * Returns metrics for the current users queries that are identified by the id
     *
     * @param queryId
     *
     * @return datawave.webservice.result.QueryMetricListResponse
     *
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @PermitAll
    @RequestMapping(path = "/id/{queryId}", method = {RequestMethod.GET}, produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public BaseQueryMetricListResponse query(@AuthenticationPrincipal ProxiedUserDetails currentUser,
                    @ApiParam("queryId to return") @PathVariable("queryId") String queryId) {
        
        BaseQueryMetricListResponse response = new QueryMetricListResponse();
        List<BaseQueryMetric> metricList = new ArrayList<>();
        try {
            BaseQueryMetric metric = incomingQueryMetricsCache.get(queryId, BaseQueryMetric.class);
            if (metric != null) {
                String adminRole = queryMetricHandlerProperties.getMetricAdminRole();
                boolean allowAllMetrics = adminRole == null;
                boolean sameUser = false;
                if (currentUser != null) {
                    String metricUser = metric.getUser();
                    String requestingUser = DnUtils.getShortName(currentUser.getPrimaryUser().getName());
                    sameUser = metricUser != null && metricUser.equals(requestingUser);
                    allowAllMetrics = allowAllMetrics || currentUser.getPrimaryUser().getRoles().contains(adminRole);
                }
                if (sameUser || allowAllMetrics) {
                    metricList.add(metric);
                }
            }
        } catch (Exception e) {
            response.addException(new QueryException(e.getMessage(), 500));
        }
        response.setResult(metricList);
        if (metricList.isEmpty()) {
            response.setHasResults(false);
        } else {
            response.setHasResults(true);
        }
        return response;
    }
}
