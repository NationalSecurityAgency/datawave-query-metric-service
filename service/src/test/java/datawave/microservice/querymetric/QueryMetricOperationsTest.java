package datawave.microservice.querymetric;

import datawave.webservice.result.VoidResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpEntity;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

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
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("http").host("localhost").port(webServicePort).path(updateMetricUrl).build();
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        long created = m.getCreateDate().getTime();
        m.addPageTime(1000, 1000, created - 1000, created);
        HttpEntity requestEntity = createRequestEntity(null, adminUser, m);
        restTemplate.postForEntity(uri.toUri(), requestEntity, VoidResponse.class);
        if (isHazelCast) {
            waitForWriteBehind(incomingQueryMetricsCache, lastWrittenQueryMetricCache, queryId);
            assertEquals("lastWrittenQueryMetricCache metric wrong", m, lastWrittenQueryMetricCache.get(queryId, BaseQueryMetric.class));
        }
        assertEquals("incomingQueryMetricsCache metric wrong", m, incomingQueryMetricsCache.get(queryId, BaseQueryMetric.class));
        assertEquals("accumulo metric wrong", m, shardTableQueryMetricHandler.getQueryMetric(queryId));
    }
    
    @Test
    public void MultipleMetricsStoredCorrectlyInCachesAndAccumulo() throws Exception {
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("http").host("localhost").port(webServicePort).path(updateMetricUrl).build();
        List<BaseQueryMetric> metrics = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String id = createQueryId();
            BaseQueryMetric m = createMetric(id);
            metrics.add(m);
            HttpEntity requestEntity = createRequestEntity(null, adminUser, m);
            restTemplate.postForEntity(uri.toUri(), requestEntity, VoidResponse.class);
        }
        metrics.forEach((m) -> {
            String queryId = m.getQueryId();
            if (isHazelCast) {
                waitForWriteBehind(incomingQueryMetricsCache, lastWrittenQueryMetricCache, queryId);
                assertEquals("lastWrittenQueryMetricCache metric wrong", m, lastWrittenQueryMetricCache.get(queryId, BaseQueryMetric.class));
            }
            assertEquals("accumulo metric wrong", m, shardTableQueryMetricHandler.getQueryMetric(queryId));
            assertEquals("incomingQueryMetricsCache metric wrong", m, incomingQueryMetricsCache.get(queryId, BaseQueryMetric.class));
        });
    }
    
    @Test
    public void MultipleMetricsAsListStoredCorrectlyInCachesAndAccumulo() throws Exception {
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("http").host("localhost").port(webServicePort).path(updateMetricsUrl).build();
        List<BaseQueryMetric> metrics = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String id = createQueryId();
            BaseQueryMetric m = createMetric(id);
            metrics.add(m);
        }
        HttpEntity requestEntity = createRequestEntity(null, adminUser, metrics);
        restTemplate.postForEntity(uri.toUri(), requestEntity, VoidResponse.class);
        
        metrics.forEach((m) -> {
            String queryId = m.getQueryId();
            if (isHazelCast) {
                waitForWriteBehind(incomingQueryMetricsCache, lastWrittenQueryMetricCache, queryId);
                assertEquals("lastWrittenQueryMetricCache metric wrong", m, lastWrittenQueryMetricCache.get(queryId, BaseQueryMetric.class));
            }
            assertEquals("incomingQueryMetricsCache metric wrong", m, incomingQueryMetricsCache.get(queryId, BaseQueryMetric.class));
            assertEquals("accumulo metric wrong", m, shardTableQueryMetricHandler.getQueryMetric(queryId));
        });
    }
}
