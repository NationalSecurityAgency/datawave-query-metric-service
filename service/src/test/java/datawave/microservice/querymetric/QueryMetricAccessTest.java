package datawave.microservice.querymetric;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.HttpClientErrorException;

import java.util.ArrayList;
import java.util.List;

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
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(createMetric())
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(nonAdminUser)
                .build());
        // @formatter:on
    }
    
    @Test(expected = HttpClientErrorException.Forbidden.class)
    public void RejectNonAdminUserForUpdateMetrics() throws Exception {
        List<BaseQueryMetric> metrics = new ArrayList<>();
        metrics.add(createMetric());
        metrics.add(createMetric());
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetrics(metrics)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(nonAdminUser)
                .build());
        // @formatter:on
    }
    
    @Test
    public void AcceptAdminUserForUpdateMetric() throws Exception {
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(createMetric())
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build());
        // @formatter:on
    }
    
    @Test
    public void AcceptAdminUserForUpdateMetrics() throws Exception {
        List<BaseQueryMetric> metrics = new ArrayList<>();
        metrics.add(createMetric());
        metrics.add(createMetric());
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetrics(metrics)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build());
        // @formatter:on
    }
}
