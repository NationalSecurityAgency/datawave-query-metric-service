package datawave.microservice.querymetric;

import com.hazelcast.map.IMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Collections;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"HazelcastCachingTest", "QueryMetricTest", "hazelcast-writethrough"})
public class HazelcastCachingTest extends QueryMetricTestBase {
    
    @BeforeEach
    public void setup() {
        super.setup();
    }
    
    @AfterEach
    public void cleanup() {
        super.cleanup();
    }
    
    @Test
    public void TestReadThroughCache() {
        
        try {
            String queryId = createQueryId();
            BaseQueryMetric m = createMetric(queryId);
            shardTableQueryMetricHandler.writeMetric(m, Collections.singletonList(m), m.getLastUpdated(), false);
            BaseQueryMetric metricFromReadThroughCache = lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdate.class).getMetric();
            assertEquals("read through cache failed", m, metricFromReadThroughCache);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Assertions.fail(e.getMessage());
        }
    }
    
    @Test
    public void TestWriteThroughCache() {
        
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        
        // use a native cache set vs Cache.put to prevent the fetching and return of Accumulo value
        ((IMap<Object,Object>) incomingQueryMetricsCache.getNativeCache()).set(queryId, new QueryMetricUpdate(m));
        try {
            BaseQueryMetric metricFromAccumulo = null;
            do {
                metricFromAccumulo = shardTableQueryMetricHandler.getQueryMetric(queryId);
            } while (metricFromAccumulo == null);
            assertEquals("write through cache failed", m, metricFromAccumulo);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Assertions.fail(e.getMessage());
        }
    }
    
    @Test
    public void InMemoryAccumuloAndCachesReset() {
        // ensure that the Hazelcast caches and in-memory Accumulo are being reset between each test
        Assertions.assertEquals(0, getAllAccumuloEntries().size(), "accumulo not empty");
    }
}
