package datawave.microservice.querymetric;

import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.function.QueryMetricSupplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.Message;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static datawave.microservice.querymetric.config.HazelcastServerConfiguration.INCOMING_METRICS;

/*
 * This class tests that a QueryMetricClient can be created and used with messaging
 * when a JWTTokenHandler is not AutoWired due to SpringBootTest.WebEnvironment.NONE
 * and ConditionalOnWebApplication in JWTConfiguration
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"NonWebApplicationMessagingTest", "QueryMetricTest"})
public class NonWebApplicationMessagingTest {
    
    @Autowired
    private QueryMetricClient client;
    
    @Autowired
    private QueryMetricFactory queryMetricFactory;
    
    @Autowired
    public List<QueryMetricUpdate> storedMetricUpdates;
    
    private Map<String,String> metricMarkings;
    
    @Before
    public void setup() {
        this.metricMarkings = new HashMap<>();
        this.metricMarkings.put(MarkingFunctions.Default.COLUMN_VISIBILITY, "A&C");
        this.storedMetricUpdates.clear();
    }
    
    /*
     * Ensure that a metric with only the queryId set will still be accepted
     */
    @Test
    public void testBareMinimumMetric() throws Exception {
        String queryId = "1111-2222-3333-4444";
        BaseQueryMetric m = queryMetricFactory.createMetric();
        m.setQueryId(queryId);
        // @formatter:off
        this.client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .build());
        QueryMetricUpdate metricUpdate = this.storedMetricUpdates.stream().filter(x -> x.getMetric().getQueryId().equals(queryId)).findAny().orElse(null);
        QueryMetricTestBase.assertEquals("", metricUpdate.getMetric(), m);
    }

    @Configuration
    @Profile("NonWebApplicationMessagingTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class QueryMetricHttpTestConfiguration {
        @Bean
        public List<QueryMetricUpdate> storedMetricUpdates() {
            return new ArrayList<>();
        }

        @Primary
        @Bean
        public QueryMetricSupplier testQueryMetricSource(@Lazy QueryMetricOperations queryMetricOperations) {
            return new QueryMetricSupplier() {
                @Override
                public boolean send(Message<QueryMetricUpdate> queryMetricUpdate) {
                    storedMetricUpdates().add(queryMetricUpdate.getPayload());
                    return true;
                }
            };
        }
    }
}
