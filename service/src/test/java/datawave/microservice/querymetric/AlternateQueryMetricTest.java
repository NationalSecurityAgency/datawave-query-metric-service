package datawave.microservice.querymetric;

import datawave.microservice.querymetric.config.AlternateQueryMetric;
import datawave.webservice.result.VoidResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"AlternateQueryMetricTest", "QueryMetricTest", "http", "hazelcast-writethrough"})
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
        
        UriComponents updateUri = UriComponentsBuilder.newInstance().scheme("http").host("localhost").port(webServicePort).path(updateMetricUrl).build();
        
        AlternateQueryMetric m = new AlternateQueryMetric();
        String queryId = createQueryId();
        populateMetric(m, queryId);
        m.setExtraField("extraValue");
        
        HttpEntity requestEntity = createRequestEntity(null, adminUser, m);
        restTemplate.postForEntity(updateUri.toUri(), requestEntity, VoidResponse.class);
        assertEquals("incomingQueryMetricsCache metric wrong", m, incomingQueryMetricsCache.get(queryId, BaseQueryMetric.class));
        assertEquals("lastWrittenQueryMetricCache metric wrong", m, lastWrittenQueryMetricCache.get(queryId, BaseQueryMetric.class));
        AlternateQueryMetric metricFromAccumulo = (AlternateQueryMetric) shardTableQueryMetricHandler.getQueryMetric(queryId);
        assertEquals("accumulo metric wrong", m, metricFromAccumulo);
        Assert.assertEquals("extra field missing/incorrect", m.getExtraField(), metricFromAccumulo.getExtraField());
    }
}
