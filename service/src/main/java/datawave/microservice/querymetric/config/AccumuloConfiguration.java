package datawave.microservice.querymetric.config;

import datawave.core.common.connection.AccumuloConnectionPool;
import datawave.microservice.config.accumulo.AccumuloProperties;
import datawave.microservice.config.cluster.ClusterProperties;
import datawave.microservice.querymetric.factory.WrappedAccumuloConnectionPoolFactory;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Configuration
@EnableConfigurationProperties({AccumuloConfiguration.WarehouseClusterProperties.class})
public class AccumuloConfiguration {
    
    @ConfigurationProperties(prefix = "warehouse-cluster")
    public static class WarehouseClusterProperties extends ClusterProperties {
        
    }
    
    @Bean
    @Lazy
    @Qualifier("warehouse")
    @ConditionalOnMissingBean
    public AccumuloProperties warehouseAccumuloProperies(WarehouseClusterProperties warehouseProperties) {
        return warehouseProperties.getAccumulo();
    }
    
    @Bean
    @Lazy
    @Qualifier("warehouse")
    @ConditionalOnMissingBean
    public AccumuloConnectionPool accumuloConnectionPool(@Qualifier("warehouse") AccumuloProperties accumuloProperties,
                    @Qualifier("warehouse") Instance instance) {
        return new AccumuloConnectionPool(new WrappedAccumuloConnectionPoolFactory(accumuloProperties, instance));
    }
    
    @Bean
    @Lazy
    @Qualifier("warehouse")
    @ConditionalOnMissingBean
    public Connector warehouseConnector(@Qualifier("warehouse") AccumuloProperties accumuloProperties) throws AccumuloSecurityException, AccumuloException {
        ZooKeeperInstance zooKeeperInstance = new ZooKeeperInstance(accumuloProperties.getInstanceName(), accumuloProperties.getZookeepers());
        return zooKeeperInstance.getConnector(accumuloProperties.getUsername(), new PasswordToken(accumuloProperties.getPassword()));
    }
    
    @Bean
    @Lazy
    @Qualifier("warehouse")
    @ConditionalOnMissingBean
    public Instance warehouseInstance(@Qualifier("warehouse") Connector connector) {
        return connector.getInstance();
    }
}
