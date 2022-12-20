package datawave.microservice.querymetric;

import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.handler.AccumuloClientTracking;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.webservice.common.connection.AccumuloClientPool;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Connector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static datawave.microservice.querymetric.config.HazelcastMetricCacheConfiguration.INCOMING_METRICS;

/*
 * This class tests that a QueryMetricClient can be created and used with messaging
 * when a JWTTokenHandler is not AutoWired due to SpringBootTest.WebEnvironment.NONE
 * and ConditionalOnWebApplication in JWTConfiguration
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext
@ActiveProfiles({"NonWebApplicationMessagingTest", "QueryMetricTest", "hazelcast-writethrough"})
public class NonWebApplicationMessagingTest {
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    @Autowired
    private QueryMetricClient client;
    
    @Autowired
    private QueryMetricFactory queryMetricFactory;
    
    @Autowired
    private MergeLockLifecycleListener mergeLockLifecycleListener;
    
    @Autowired
    protected QueryMetricHandlerProperties queryMetricHandlerProperties;
    
    @Autowired
    private ShardTableQueryMetricHandler shardTableQueryMetricHandler;
    
    @Autowired
    protected @Qualifier("warehouse") AccumuloClientPool accumuloClientPool;
    
    @Autowired
    @Named("queryMetricCacheManager")
    protected CacheManager cacheManager;
    
    private Cache incomingQueryMetricsCache;
    
    @Before
    public void setup() {
        this.incomingQueryMetricsCache = cacheManager.getCache(INCOMING_METRICS);
        this.mergeLockLifecycleListener.setAllowReadLock(true);
        
        BaseQueryMetric m = queryMetricFactory.createMetric();
        m.setQueryId(QueryMetricTestBase.createQueryId());
        // this is to ensure that the QueryMetrics_m table
        // is populated so that queries work properly
        try {
            this.shardTableQueryMetricHandler.writeMetric(m, Collections.singletonList(m), m.getLastUpdated(), false);
            this.shardTableQueryMetricHandler.flush();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        List<String> auths = Arrays.asList("PUBLIC", "A", "B", "C");
        List<String> tables = new ArrayList<>();
        tables.add(queryMetricHandlerProperties.getIndexTableName());
        tables.add(queryMetricHandlerProperties.getReverseIndexTableName());
        tables.add(queryMetricHandlerProperties.getShardTableName());
        AccumuloClient accumuloClient = null;
        try {
            Map<String,String> trackingMap = AccumuloClientTracking.getTrackingMap(Thread.currentThread().getStackTrace());
            accumuloClient = this.accumuloClientPool.borrowObject(trackingMap);
            QueryMetricTestBase.deleteAccumuloEntries(accumuloClient, tables, auths);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (accumuloClient != null) {
                this.accumuloClientPool.returnObject(accumuloClient);
            }
        }
    }
    
    /*
     * Ensure that a metric with only the queryId set will still be accepted
     */
    @Test
    public void testBareMinimumMetric() throws Exception {
        String queryId = "1111-2222-3333-4444";
        BaseQueryMetric m = queryMetricFactory.createMetric();
        m.setQueryId(queryId);
        // @formatter:off
        this.client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .build());
        QueryMetricUpdateHolder metricUpdate = this.incomingQueryMetricsCache.get(queryId, QueryMetricUpdateHolder.class);
        QueryMetricTestBase.assertEquals("", metricUpdate.getMetric(), m);
    }
}
