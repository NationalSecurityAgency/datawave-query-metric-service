package datawave.microservice.querymetric;

import datawave.microservice.querymetric.config.CorrelatorProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"CorrelatorTest", "QueryMetricTest", "hazelcast-writebehind", "correlator"})
public class CorrelatorTest extends QueryMetricTestBase {
    
    @Autowired
    private QueryMetricOperations queryMetricOperations;
    
    @Autowired
    private CorrelatorProperties correlatorProperties;
    
    @Autowired
    private Correlator correlator;
    
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void cleanup() {
        super.cleanup();
    }
    
    @Test
    public void TestSizeLimitedQueue() throws Exception {
        this.correlatorProperties.setMaxCorrelationTimeMs(30000);
        this.correlatorProperties.setMaxCorrelationQueueSize(95);
        testMetricsCorrelated(100, 100);
    }
    
    @Test
    public void TestTimeLimitedQueue() throws Exception {
        this.correlatorProperties.setMaxCorrelationTimeMs(500);
        this.correlatorProperties.setMaxCorrelationQueueSize(100);
        testMetricsCorrelated(100, 100);
    }
    
    @Test
    public void TestNoStorageUntilShutdown() throws Exception {
        this.correlatorProperties.setMaxCorrelationTimeMs(60000);
        this.correlatorProperties.setMaxCorrelationQueueSize(100);
        testMetricsCorrelated(100, 10);
    }
    
    @Test
    public void TestManyQueries() throws Exception {
        this.correlatorProperties.setMaxCorrelationTimeMs(500);
        this.correlatorProperties.setMaxCorrelationQueueSize(100);
        testMetricsCorrelated(1000, 20);
    }
    
    public void testMetricsCorrelated(int numMetrics, int maxPages) throws Exception {
        List<BaseQueryMetric> updates = new ArrayList<>();
        List<BaseQueryMetric> metrics = new ArrayList<>();
        for (int x = 0; x < numMetrics; x++) {
            BaseQueryMetric m = createMetric();
            metrics.add(m);
            updates.add(m.duplicate());
        }
        
        List<BaseQueryMetric> shuffledUpdates = new ArrayList<>();
        Random r = new Random();
        for (BaseQueryMetric m : metrics) {
            int numPages = r.nextInt(maxPages);
            BaseQueryMetric m2 = m;
            for (int x = 0; x < numPages; x++) {
                m2 = m2.duplicate();
                m.setLifecycle(BaseQueryMetric.Lifecycle.RESULTS);
                m2.setLifecycle(BaseQueryMetric.Lifecycle.RESULTS);
                BaseQueryMetric.PageMetric pageMetric = new BaseQueryMetric.PageMetric("localhost", 100, 100, 100, 100, -1, -1, -1, -1);
                m.addPageMetric(pageMetric);
                m2.addPageMetric(pageMetric);
                shuffledUpdates.add(m2);
            }
        }
        
        // randomize the order of metric updates
        Collections.shuffle(shuffledUpdates);
        updates.addAll(shuffledUpdates);
        
        LinkedBlockingDeque<BaseQueryMetric> updateDeque = new LinkedBlockingDeque<>();
        updateDeque.addAll(updates);
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (int x = 0; x < 20; x++) {
            Runnable runnable = () -> {
                BaseQueryMetric m;
                do {
                    m = updateDeque.poll();
                    if (m != null) {
                        queryMetricOperations.handleEvent(new QueryMetricUpdate(m, QueryMetricType.COMPLETE));
                    }
                } while (m != null);
            };
            executorService.submit(runnable);
        }
        log.debug("done submitting metrics");
        executorService.shutdown();
        boolean completed = executorService.awaitTermination(2, TimeUnit.MINUTES);
        Assert.assertTrue("executor tasks completed", completed);
        // flush the correlator
        this.correlator.shutdown(true);
        this.queryMetricOperations.ensureUpdatesProcessed();
        this.correlator.shutdown(false);
        
        long start = System.currentTimeMillis();
        for (BaseQueryMetric m : metrics) {
            String queryId = m.getQueryId();
            ensureDataStored(incomingQueryMetricsCache, queryId);
            QueryMetricUpdate metricUpdate;
            BaseQueryMetric storedMetric = null;
            do {
                metricUpdate = incomingQueryMetricsCache.get(queryId, QueryMetricUpdate.class);
                if (metricUpdate == null && (System.currentTimeMillis() - start) < 5000) {
                    Thread.sleep(200);
                } else {
                    storedMetric = metricUpdate.getMetric();
                }
            } while (storedMetric == null);
            Assert.assertNotNull("missing metric " + queryId, storedMetric);
            assertEquals("incomingQueryMetricsCache metric wrong for id:" + m.getQueryId(), m, storedMetric);
        }
    }
}
