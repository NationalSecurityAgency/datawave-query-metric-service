package datawave.microservice.querymetric;

import com.hazelcast.map.impl.proxy.MapProxyImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"HazelcastCachingTest", "QueryMetricTest", "http", "hazelcast-writethrough"})
public class HazelcastCachingTest extends QueryMetricTestBase {
    
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void cleanup() {
        super.cleanup();
    }
    
    @Test
    public void TestReadThroughCache() {
        
        try {
            String queryId = createQueryId();
            BaseQueryMetric m = createMetric(queryId);
            shardTableQueryMetricHandler.writeMetric(m, Collections.singletonList(m), m.getLastUpdated(), false);
            BaseQueryMetric metricFromReadThroughCache = lastWrittenQueryMetricCache.get(queryId, BaseQueryMetric.class);
            assertEquals("read through cache failed", m, metricFromReadThroughCache);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Assert.fail(e.getMessage());
        }
    }
    
    @Test
    public void TestWriteThroughCache() {
        
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        
        // use a native cache set vs Cache.put to prevent the fetching and return of Accumulo value
        ((MapProxyImpl) incomingQueryMetricsCache.getNativeCache()).set(queryId, m);
        BaseQueryMetric metricFromAccumulo = null;
        do {
            metricFromAccumulo = shardTableQueryMetricHandler.getQueryMetric(queryId);
        } while (metricFromAccumulo == null);
        assertEquals("write through cache failed", m, metricFromAccumulo);
    }
    
    @Test
    public void InMemoryAccumuloAndCachesReset() {
        // ensure that the Hazelcast caches and in-memory Accumulo are being reset between each test
        Assert.assertEquals("accumulo not empty", 0, getAllAccumuloEntries().size());
    }
}
