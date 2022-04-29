package datawave.microservice.querymetric;

import datawave.microservice.querymetric.config.AlternateQueryMetric;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"AlternateQueryMetricTest", "QueryMetricTest", "MessageRouting", "hazelcast-writethrough"})
public class AlternateQueryMetricTest extends QueryMetricTestBase {
    
    @BeforeEach
    public void setup() {
        super.setup();
    }
    
    @AfterEach
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
        Assertions.assertEquals(m.getExtraField(), metricFromAccumulo.getExtraField(), "extra field missing/incorrect");
    }
}
