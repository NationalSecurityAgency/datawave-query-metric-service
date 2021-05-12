package datawave.microservice.querymetric;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public abstract class QueryMetricOperationsTest extends QueryMetricTestBase {
    
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void cleanup() {
        super.cleanup();
    }
    
    @Test
    public void MetricStoredCorrectlyInCachesAndAccumulo() throws Exception {
        
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        long created = m.getCreateDate().getTime();
        m.addPageTime("localhost", 1000, 1000, created - 1000, created);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build());
        // @formatter:on
        if (isHazelCast) {
            waitForWriteBehind(incomingQueryMetricsCache, lastWrittenQueryMetricCache, queryId);
            assertEquals("lastWrittenQueryMetricCache metric wrong", m, lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdate.class).getMetric());
        }
        assertEquals("incomingQueryMetricsCache metric wrong", m, incomingQueryMetricsCache.get(queryId, QueryMetricUpdate.class).getMetric());
        assertEquals("accumulo metric wrong", m, shardTableQueryMetricHandler.getQueryMetric(queryId));
    }
    
    @Test
    public void MultipleMetricsStoredCorrectlyInCachesAndAccumulo() throws Exception {
        
        List<BaseQueryMetric> metrics = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String id = createQueryId();
            BaseQueryMetric m = createMetric(id);
            metrics.add(m);
            // @formatter:off
            client.submit(new QueryMetricClient.Request.Builder()
                    .withMetric(m)
                    .withMetricType(QueryMetricType.COMPLETE)
                    .withUser(adminUser)
                    .build());
            // @formatter:on
        }
        metrics.forEach((m) -> {
            String queryId = m.getQueryId();
            if (isHazelCast) {
                waitForWriteBehind(incomingQueryMetricsCache, lastWrittenQueryMetricCache, queryId);
                assertEquals("lastWrittenQueryMetricCache metric wrong", m, lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdate.class).getMetric());
            }
            assertEquals("accumulo metric wrong", m, shardTableQueryMetricHandler.getQueryMetric(queryId));
            assertEquals("incomingQueryMetricsCache metric wrong", m, incomingQueryMetricsCache.get(queryId, QueryMetricUpdate.class).getMetric());
        });
    }
    
    @Test
    public void MultipleMetricsAsListStoredCorrectlyInCachesAndAccumulo() throws Exception {
        
        List<BaseQueryMetric> metrics = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String id = createQueryId();
            BaseQueryMetric m = createMetric(id);
            metrics.add(m);
        }
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetrics(metrics)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build());
        // @formatter:on
        metrics.forEach((m) -> {
            String queryId = m.getQueryId();
            if (isHazelCast) {
                waitForWriteBehind(incomingQueryMetricsCache, lastWrittenQueryMetricCache, queryId);
                assertEquals("lastWrittenQueryMetricCache metric wrong", m, lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdate.class).getMetric());
            }
            assertEquals("incomingQueryMetricsCache metric wrong", m, incomingQueryMetricsCache.get(queryId, QueryMetricUpdate.class).getMetric());
            assertEquals("accumulo metric wrong", m, shardTableQueryMetricHandler.getQueryMetric(queryId));
        });
    }
}
