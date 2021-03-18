package datawave.microservice.querymetrics;

import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.metric.BaseQueryMetricListResponse;
import datawave.webservice.result.VoidResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"PageMetricTest", "QueryMetricTest", "hazelcast-writethrough"})
public class PageMetricTest extends QueryMetricTestBase {
    
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void cleanup() {
        super.cleanup();
    }
    
    @Test
    public void PageMetricTest() throws Exception {
        int port = webServicePort;
        UriComponents updateUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(port).path(updateMetricUrl).build();
        
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        
        int numPages = 10;
        for (int i = 0; i < numPages; i++) {
            long now = System.currentTimeMillis();
            m.addPageTime(1000, 1000, now - 1000, now);
            HttpEntity updateRequestEntity = createRequestEntity(null, adminUser, m);
            restTemplate.postForEntity(updateUri.toUri(), updateRequestEntity, VoidResponse.class);
            UriComponents metricUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(port)
                            .path(String.format(getMetricsUrl, queryId)).build();
            
            HttpEntity metricRequestEntity = createRequestEntity(null, adminUser, null);
            ResponseEntity<BaseQueryMetricListResponse> metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity,
                            BaseQueryMetricListResponse.class);
            
            Assert.assertEquals(1, metricResponse.getBody().getNumResults());
            BaseQueryMetric returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
            Assert.assertEquals(i + 1, returnedMetric.getPageTimes().size());
            assertEquals(m, returnedMetric);
        }
    }
}
