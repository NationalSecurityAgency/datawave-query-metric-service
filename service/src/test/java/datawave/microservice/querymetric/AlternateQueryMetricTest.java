package datawave.microservice.querymetric;

import datawave.microservice.querymetric.alternate.AlternateQueryMetric;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"AlternateQueryMetricTest", "QueryMetricTest", "hazelcast-writethrough"})
public class AlternateQueryMetricTest extends QueryMetricTestBase {
    
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void cleanup() {
        super.cleanup();
    }
    
    @Test
    public void AlternateMetricDeserializedAndStoredCorrectly() throws Exception {
        
        AlternateQueryMetric m = new AlternateQueryMetric();
        String queryId = createQueryId();
        populateMetric(m, queryId);
        m.setExtraField("extraValue");
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build());
        // @formatter:on
        assertEquals("incomingQueryMetricsCache metric wrong", m, incomingQueryMetricsCache.get(queryId, QueryMetricUpdate.class).getMetric());
        assertEquals("lastWrittenQueryMetricCache metric wrong", m, lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdate.class).getMetric());
        AlternateQueryMetric metricFromAccumulo = (AlternateQueryMetric) shardTableQueryMetricHandler.getQueryMetric(queryId);
        assertEquals("accumulo metric wrong", m, metricFromAccumulo);
        Assert.assertEquals("extra field missing/incorrect", m.getExtraField(), metricFromAccumulo.getExtraField());
    }
}
