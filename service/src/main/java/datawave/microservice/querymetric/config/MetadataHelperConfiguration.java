package datawave.microservice.querymetric.config;

import com.github.benmanes.caffeine.cache.CaffeineSpec;
import datawave.microservice.querymetric.handler.AccumuloConnectionTracking;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.TypeMetadataHelper;
import datawave.webservice.common.connection.AccumuloConnectionPool;
import org.apache.accumulo.core.security.Authorizations;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static datawave.query.util.MetadataHelperFactory.ALL_AUTHS_PROPERTY;

@Configuration
@EnableConfigurationProperties({MetadataHelperProperties.class})
public class MetadataHelperConfiguration {
    
    // This bean is used via autowire in AllFieldMetadataHelper
    @Bean(name = "metadataHelperCacheManager")
    public CaffeineCacheManager metadataHelperCacheManager(QueryMetricHandlerProperties queryMetricHandlerProperties) {
        System.setProperty(ALL_AUTHS_PROPERTY, queryMetricHandlerProperties.getMetadataDefaultAuths());
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeineSpec(CaffeineSpec.parse("maximumSize=100, expireAfterAccess=24h, expireAfterWrite=24h"));
        return caffeineCacheManager;
    }
    
    // This bean is used via autowire in DateIndexHelper
    @Bean(name = "dateIndexHelperCacheManager")
    public CaffeineCacheManager dateIndexHelperCacheManager(QueryMetricHandlerProperties queryMetricHandlerProperties) {
        System.setProperty(ALL_AUTHS_PROPERTY, queryMetricHandlerProperties.getMetadataDefaultAuths());
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeineSpec(CaffeineSpec.parse("maximumSize=1000, expireAfterAccess=24h, expireAfterWrite=24h"));
        return caffeineCacheManager;
    }
    
    // This bean is used to override the Hazelcast cacheManager bean that would otherwise be created in
    // HazelcastCacheConfiguration which is included from datawave-spring-boot-starter-cache
    // This bean is used via autowire in AllFieldMetadataHelper
    @Primary
    @Bean(name = "cacheManager")
    public CaffeineCacheManager cacheManager(QueryMetricHandlerProperties queryMetricHandlerProperties) {
        System.setProperty(ALL_AUTHS_PROPERTY, queryMetricHandlerProperties.getMetadataDefaultAuths());
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeineSpec(CaffeineSpec.parse("maximumSize=1000, expireAfterAccess=24h, expireAfterWrite=24h"));
        return caffeineCacheManager;
    }
    
    @Bean
    @ConditionalOnMissingBean
    public DateIndexHelperFactory dateIndexHelperFactory() {
        DateIndexHelper dateIndexHelper = DateIndexHelper.getInstance();
        return new DateIndexHelperFactory() {
            @Override
            public DateIndexHelper createDateIndexHelper() {
                return dateIndexHelper;
            }
        };
    }
    
    @Bean
    @Qualifier("queryMetrics")
    public TypeMetadataHelper.Factory typeMetadataFactory(ApplicationContext context) {
        return new TypeMetadataHelper.Factory(context);
    }
    
    @Bean
    @Qualifier("allMetadataAuths")
    public Set<Authorizations> allMetadataAuths(MetadataHelperProperties metadataHelperProperties) {
        Set<Authorizations> allMetadataAuths = new HashSet<>();
        metadataHelperProperties.getAllMetadataAuths().forEach(a -> {
            allMetadataAuths.add(new Authorizations(a));
        });
        return allMetadataAuths;
    }
    
    @Bean
    @Qualifier("typeSubstitutions")
    public Map<String,String> typeSubstitutions(MetadataHelperProperties metadataHelperProperties) {
        return metadataHelperProperties.getTypeSubstitutions();
    }
    
    @Bean
    @Qualifier("metadataTableName")
    public String metadataTableName(QueryMetricHandlerProperties queryMetricHandlerProperties) {
        return queryMetricHandlerProperties.getMetadataTableName();
    }
    
    @Bean
    @ConditionalOnMissingBean
    public CompositeMetadataHelper compositeMetadataHelper(@Qualifier("warehouse") AccumuloConnectionPool connectionPool,
                    @Qualifier("metadataTableName") String metadataTableName, @Qualifier("allMetadataAuths") Set<Authorizations> allMetadataAuths)
                    throws Exception {
        Map<String,String> trackingMap = AccumuloConnectionTracking.getTrackingMap(Thread.currentThread().getStackTrace());
        return new CompositeMetadataHelper(connectionPool.borrowObject(trackingMap), metadataTableName, allMetadataAuths);
    }
}
