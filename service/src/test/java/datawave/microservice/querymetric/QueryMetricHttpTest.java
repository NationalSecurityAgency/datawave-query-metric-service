package datawave.microservice.querymetric;

import datawave.microservice.querymetric.alternate.AlternateQueryMetricSupplier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.HttpClientErrorException;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryMetricHttpTest", "QueryMetricTest", "http", "hazelcast-writebehind"})
public class QueryMetricHttpTest extends QueryMetricTestBase {
    
    @Autowired
    private AlternateQueryMetricSupplier queryMetricSupplier;
    
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void cleanup() {
        super.cleanup();
        queryMetricSupplier.reset();
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
    
    // Messages that arrive via http/https get placed on the message bus
    // to ensure a quick response and to maintain a single queue of work
    // The AlternateQueryMetricSupplier gets AutoWired here and in QueryMetricOperations
    // and is used to place the metric update on the message bus
    @Test
    public void HttpUpdateOnMessageBus() throws Exception {
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
        Assert.assertEquals(2, queryMetricSupplier.getMessagesSent());
    }
}
