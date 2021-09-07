package datawave.microservice.querymetric.config;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.microservice.config.accumulo.AccumuloProperties;
import datawave.microservice.querymetric.alternate.AlternateQueryMetricSupplier;
import datawave.microservice.querymetric.factory.WrappedAccumuloConnectionPoolFactory;
import datawave.webservice.common.connection.AccumuloConnectionPool;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@ImportAutoConfiguration({RefreshAutoConfiguration.class})
@AutoConfigureCache(cacheProvider = CacheType.HAZELCAST)
@ComponentScan(basePackages = "datawave.microservice")
@Profile("QueryMetricTest")
@Configuration
public class QueryMetricTestConfiguration {
    
    @Autowired
    @Qualifier("warehouse")
    private AccumuloProperties accumuloProperties;
    
    public QueryMetricTestConfiguration() {}
    
    @Bean
    @Lazy
    @Qualifier("warehouse")
    public AccumuloConnectionPool accumuloConnectionPool(@Qualifier("warehouse") AccumuloProperties accumuloProperties,
                    @Qualifier("warehouse") Instance instance) {
        return new AccumuloConnectionPool(new WrappedAccumuloConnectionPoolFactory(accumuloProperties, instance));
    }
    
    @Bean
    @Lazy
    @Qualifier("warehouse")
    public Instance memoryWarehouseInstance() throws Exception {
        Instance instance = new InMemoryInstance();
        Connector connector = instance.getConnector(accumuloProperties.getUsername(), new PasswordToken(accumuloProperties.getPassword()));
        connector.securityOperations().changeUserAuthorizations(connector.whoami(), new Authorizations("PUBLIC", "A", "B", "C"));
        return instance;
    }
    
    @Bean
    @Lazy
    @Qualifier("warehouse")
    public Connector memoryWarehouseConnector(@Qualifier("warehouse") Instance instance) throws AccumuloSecurityException, AccumuloException {
        return instance.getConnector(accumuloProperties.getUsername(), new PasswordToken(accumuloProperties.getPassword()));
    }
    
    @Bean
    @Primary
    public AlternateQueryMetricSupplier alternateQueryMetricSupplier() {
        return new AlternateQueryMetricSupplier();
    }
}
