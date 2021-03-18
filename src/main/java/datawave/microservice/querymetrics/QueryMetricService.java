package datawave.microservice.querymetrics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * Launcher for the query metric service.
 */
@EnableDiscoveryClient
@SpringBootApplication(scanBasePackages = {"datawave.microservice", "datawave.query.util"}, exclude = {ErrorMvcAutoConfiguration.class})
public class QueryMetricService {
    public static void main(String[] args) {
        SpringApplication.run(QueryMetricService.class, args);
    }
}
