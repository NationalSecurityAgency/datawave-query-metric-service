package datawave.microservice.querymetric;

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
@ActiveProfiles({"MessagingTest", "QueryMetricTest", "hazelcast-writethrough"})
public class MessagingTest extends QueryMetricTestBase {
    
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void cleanup() {
        super.cleanup();
    }
    
    @Test
    public void sendMetricViaMessage() throws Exception {
        int port = webServicePort;
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .build());

        ensureDataWritten(incomingQueryMetricsCache, lastWrittenQueryMetricCache, queryId);
        // @formatter:on
        UriComponents metricUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(port).path(String.format(getMetricsUrl, queryId))
                        .build();
        HttpEntity metricRequestEntity = createRequestEntity(null, adminUser, null);
        
        ResponseEntity<BaseQueryMetricListResponse> metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity,
                        BaseQueryMetricListResponse.class);
        
        Assert.assertEquals(1, metricResponse.getBody().getNumResults());
        BaseQueryMetric returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
        assertEquals(m, returnedMetric);
    }
    
    @Test
    public void sendMultiplePagesViaMessage() throws Exception {
        int port = webServicePort;
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        UriComponents metricUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(port).path(String.format(getMetricsUrl, queryId))
                        .build();
        HttpEntity metricRequestEntity = createRequestEntity(null, adminUser, null);
        
        int numPages = 10;
        for (int i = 0; i < numPages; i++) {
            long now = System.currentTimeMillis();
            m.addPageTime("localhost", 1000, 1000, now - 1000, now);
            
            // @formatter:off
            client.submit(new QueryMetricClient.Request.Builder()
                    .withMetric(m)
                    .withMetricType(QueryMetricType.COMPLETE)
                    .build());
            // @formatter:on
            
            ResponseEntity<BaseQueryMetricListResponse> metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity,
                            BaseQueryMetricListResponse.class);
            
            Assert.assertEquals(1, metricResponse.getBody().getNumResults());
            BaseQueryMetric returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
            Assert.assertEquals(i + 1, returnedMetric.getPageTimes().size());
            assertEquals(m, returnedMetric);
        }
    }
}
