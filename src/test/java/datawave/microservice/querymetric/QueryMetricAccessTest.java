package datawave.microservice.querymetric;

import datawave.webservice.result.VoidResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Collections;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryMetricAccessTest", "QueryMetricTest", "http", "hazelcast-writethrough"})
public class QueryMetricAccessTest extends QueryMetricTestBase {
    
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void cleanup() {
        super.cleanup();
    }
    
    @Test(expected = HttpClientErrorException.Forbidden.class)
    public void RejectNonAdminUserForUpdateMetric() throws Exception {
        
        UriComponents updateUri = UriComponentsBuilder.newInstance().scheme("http").host("localhost").port(webServicePort).path(updateMetricUrl).build();
        HttpEntity requestEntity = createRequestEntity(null, nonAdminUser, createMetric());
        restTemplate.postForEntity(updateUri.toUri(), requestEntity, VoidResponse.class);
    }
    
    @Test(expected = HttpClientErrorException.Forbidden.class)
    public void RejectNonAdminUserForUpdateMetrics() throws Exception {
        
        UriComponents updateUri = UriComponentsBuilder.newInstance().scheme("http").host("localhost").port(webServicePort).path(updateMetricsUrl).build();
        HttpEntity requestEntity = createRequestEntity(null, nonAdminUser, Collections.singletonList(createMetric()));
        restTemplate.postForEntity(updateUri.toUri(), requestEntity, VoidResponse.class);
    }
    
    @Test
    public void AcceptAdminUserForUpdateMetric() throws Exception {
        
        UriComponents updateUri = UriComponentsBuilder.newInstance().scheme("http").host("localhost").port(webServicePort).path(updateMetricUrl).build();
        HttpEntity requestEntity = createRequestEntity(null, adminUser, createMetric());
        restTemplate.postForEntity(updateUri.toUri(), requestEntity, VoidResponse.class);
    }
    
    @Test
    public void AcceptAdminUserForUpdateMetrics() throws Exception {
        
        UriComponents updateUri = UriComponentsBuilder.newInstance().scheme("http").host("localhost").port(webServicePort).path(updateMetricsUrl).build();
        HttpEntity requestEntity = createRequestEntity(null, adminUser, Collections.singletonList(createMetric()));
        restTemplate.postForEntity(updateUri.toUri(), requestEntity, VoidResponse.class);
    }
}
