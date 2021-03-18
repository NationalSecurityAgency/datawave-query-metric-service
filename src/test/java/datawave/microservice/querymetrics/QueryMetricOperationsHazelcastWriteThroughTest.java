package datawave.microservice.querymetrics;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

/*
 * Run the same tests as in the base class but with specific profile / settings
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryMetricOperationsHazelcastWriteThroughTest", "QueryMetricTest", "http", "hazelcast-writethrough"})
public class QueryMetricOperationsHazelcastWriteThroughTest extends QueryMetricOperationsTest {
    
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void cleanup() {
        super.cleanup();
    }
    
    @Test
    @Override
    public void MetricStoredCorrectlyInCachesAndAccumulo() throws Exception {
        super.MetricStoredCorrectlyInCachesAndAccumulo();
    }
    
    @Test
    @Override
    public void MultipleMetricsStoredCorrectlyInCachesAndAccumulo() throws Exception {
        super.MultipleMetricsStoredCorrectlyInCachesAndAccumulo();
    }
    
    @Test
    @Override
    public void MultipleMetricsAsListStoredCorrectlyInCachesAndAccumulo() throws Exception {
        super.MultipleMetricsAsListStoredCorrectlyInCachesAndAccumulo();
    }
}
