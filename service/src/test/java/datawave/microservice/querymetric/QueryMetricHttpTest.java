package datawave.microservice.querymetric;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.messaging.DirectWithAttributesChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.HttpClientErrorException;

import java.util.ArrayList;
import java.util.List;

import static datawave.microservice.querymetric.config.QueryMetricSourceConfiguration.QueryMetricSourceBinding.SOURCE_NAME;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryMetricHttpTest", "QueryMetricTest", "http", "hazelcast-writebehind"})
public class QueryMetricHttpTest extends QueryMetricTestBase {
    
    @Output(SOURCE_NAME)
    @Autowired
    private MessageChannel output;
    
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void cleanup() {
        super.cleanup();
        ((DirectWithAttributesChannel) output).reset();
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
    
    // Messages that arrive via http/https get placed on the message queue
    // to ensure a quick response and to maintain a single queue of work
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
        Assert.assertEquals(2, ((DirectWithAttributesChannel) output).getSendCount());
    }
}
