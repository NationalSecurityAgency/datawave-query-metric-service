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

import java.util.Date;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryMetricConsistencyTest", "QueryMetricTest", "hazelcast-writethrough"})
public class QueryMetricConsistencyTest extends QueryMetricTestBase {
    
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
                    .withUser(adminUser)
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
    
    @Test
    public void OutOfOrderLifecycleTest() throws Exception {
        int port = webServicePort;
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        UriComponents metricUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(port).path(String.format(getMetricsUrl, queryId))
                        .build();
        m.setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build());
        // @formatter:on
        HttpEntity metricRequestEntity = createRequestEntity(null, adminUser, null);
        ResponseEntity<BaseQueryMetricListResponse> metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity,
                        BaseQueryMetricListResponse.class);
        
        Assert.assertEquals(1, metricResponse.getBody().getNumResults());
        BaseQueryMetric returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
        Assert.assertEquals("lifecycle incorrect", BaseQueryMetric.Lifecycle.CLOSED, returnedMetric.getLifecycle());
        assertEquals(m, returnedMetric);
        
        // send an update with out-of-sequence lifecycle
        m = createMetric(queryId);
        m.setLifecycle(BaseQueryMetric.Lifecycle.INITIALIZED);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build());
        // @formatter:on
        metricRequestEntity = createRequestEntity(null, adminUser, null);
        metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity, BaseQueryMetricListResponse.class);
        
        Assert.assertEquals(1, metricResponse.getBody().getNumResults());
        returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
        // metric should have been updated without backtracking on the lifecycle
        Assert.assertEquals("lifecycle incorrect", BaseQueryMetric.Lifecycle.CLOSED, returnedMetric.getLifecycle());
    }
    
    @Test
    public void DistributedUpdateTest() throws Exception {
        int port = webServicePort;
        String queryId = createQueryId();
        UriComponents metricUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(port).path(String.format(getMetricsUrl, queryId))
                        .build();
        
        long now = System.currentTimeMillis();
        BaseQueryMetric m = createMetric(queryId);
        m.setCreateDate(new Date(now));
        m.setLastUpdated(new Date(now));
        m.setSourceCount(100);
        m.setNextCount(100);
        m.setSeekCount(100);
        m.setYieldCount(100);
        m.setDocRanges(100);
        m.setFiRanges(100);
        BaseQueryMetric.PageMetric pm = new BaseQueryMetric.PageMetric("localhost", 1000, 1000, 1000, 1000, 2000, 0, 0, -1);
        pm.setPageNumber(1);
        m.addPageMetric(pm);
        m.setLifecycle(BaseQueryMetric.Lifecycle.INITIALIZED);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.DISTRIBUTED)
                .withUser(adminUser)
                .build());
        // @formatter:on
        m = createMetric(queryId);
        m.setCreateDate(new Date(now - 1000));
        m.setLastUpdated(new Date(now - 1000));
        m.setSourceCount(100);
        m.setNextCount(100);
        m.setSeekCount(100);
        m.setYieldCount(100);
        m.setDocRanges(100);
        m.setFiRanges(100);
        pm = new BaseQueryMetric.PageMetric("localhost", 1000, 1000, 1000, 1000, 2000, 0, 0, -1);
        pm.setPageNumber(1);
        m.addPageMetric(pm);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.DISTRIBUTED)
                .withUser(adminUser)
                .build());
        // @formatter:on
        HttpEntity metricRequestEntity = createRequestEntity(null, adminUser, null);
        ResponseEntity<BaseQueryMetricListResponse> metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity,
                        BaseQueryMetricListResponse.class);
        
        Assert.assertEquals(1, metricResponse.getBody().getNumResults());
        BaseQueryMetric returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
        Assert.assertEquals("create date should not change", new Date(now), returnedMetric.getLastUpdated());
        Assert.assertEquals("last updated should only increase", new Date(now), returnedMetric.getLastUpdated());
        Assert.assertEquals("source count should be additive", 200, returnedMetric.getSourceCount());
        Assert.assertEquals("next count should be additive", 200, returnedMetric.getNextCount());
        Assert.assertEquals("seek count should be additive", 200, returnedMetric.getSeekCount());
        Assert.assertEquals("yield count should be additive", 200, returnedMetric.getYieldCount());
        Assert.assertEquals("doc ranges count should be additive", 200, returnedMetric.getDocRanges());
        Assert.assertEquals("fi ranges should be additive", 200, returnedMetric.getFiRanges());
        long lastPageNumReturned = shardTableQueryMetricHandler.getLastPageNumber(returnedMetric);
        Assert.assertEquals("distributed update should append pages", 2, lastPageNumReturned);
        
        m.setLastUpdated(new Date(now + 1000));
        m.setSourceCount(1000);
        m.setNextCount(1000);
        m.setSeekCount(1000);
        m.setYieldCount(1000);
        m.setDocRanges(1000);
        m.setFiRanges(1000);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build());
        // @formatter:on
        metricRequestEntity = createRequestEntity(null, adminUser, null);
        metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity, BaseQueryMetricListResponse.class);
        
        Assert.assertEquals(1, metricResponse.getBody().getNumResults());
        returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
        Assert.assertEquals("last updated should only increase", new Date(now + 1000), returnedMetric.getLastUpdated());
        Assert.assertEquals("latest source count should be used", 1000, returnedMetric.getSourceCount());
        Assert.assertEquals("latest next count should be used", 1000, returnedMetric.getNextCount());
        Assert.assertEquals("latest seek count should be used", 1000, returnedMetric.getSeekCount());
        Assert.assertEquals("latest yield count should be used", 1000, returnedMetric.getYieldCount());
        Assert.assertEquals("latest doc ranges count should be used", 1000, returnedMetric.getDocRanges());
        Assert.assertEquals("latest fi ranges should be used", 1000, returnedMetric.getFiRanges());
    }
    
}
