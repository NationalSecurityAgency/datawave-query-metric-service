package datawave.microservice.querymetric.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import com.github.benmanes.caffeine.cache.CaffeineSpec;
import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.handler.AccumuloConnectionTracking;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.TypeMetadataHelper;
import datawave.webservice.common.connection.AccumuloConnectionPool;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static datawave.query.util.MetadataHelperFactory.ALL_AUTHS_PROPERTY;

@Configuration
@EnableConfigurationProperties({QueryMetricHandlerProperties.class, MetadataProperties.class, TimelyProperties.class})
public class QueryMetricConfiguration {
    
    @Bean
    public ObjectMapper objectMapper(QueryMetricFactory metricFactory) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
        SimpleModule module = new SimpleModule("BaseQueryMetricMapping");
        module.addAbstractTypeMapping(BaseQueryMetric.class, metricFactory.createMetric().getClass());
        mapper.registerModule(module);
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new JaxbAnnotationModule());
        return mapper;
    }
    
    @Bean
    public ResponseObjectFactory responseObjectFactory() {
        return new DefaultResponseObjectFactory();
    }
    
    @Bean(name = "metadataHelperCacheManager")
    @ConditionalOnMissingBean(name = "metadataHelperCacheManager")
    public CacheManager metadataHelperCacheManager(QueryMetricHandlerProperties queryMetricHandlerProperties) {
        System.setProperty(ALL_AUTHS_PROPERTY, queryMetricHandlerProperties.getMetadataDefaultAuths());
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeineSpec(CaffeineSpec.parse("maximumSize=100, expireAfterAccess=24h, expireAfterWrite=24h"));
        return caffeineCacheManager;
    }
    
    @Bean
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
    @ConditionalOnMissingBean
    QueryMetricFactory queryMetricFactory() {
        return new QueryMetricFactoryImpl();
    }
    
    @Bean
    @ConditionalOnMissingBean
    datawave.webservice.query.cache.QueryMetricFactory datawaveQueryMetricFactory() {
        return new datawave.webservice.query.cache.QueryMetricFactoryImpl();
    }
    
    @Bean
    @ConditionalOnMissingBean
    public ShardTableQueryMetricHandler shardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties,
                    @Qualifier("warehouse") AccumuloConnectionPool connectionPool, QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory,
                    MarkingFunctions markingFunctions) {
        datawave.webservice.query.cache.QueryMetricFactory datawaveQueryMetricFactory = new datawave.webservice.query.cache.QueryMetricFactoryImpl();
        return new ShardTableQueryMetricHandler(queryMetricHandlerProperties, connectionPool, logicFactory, metricFactory, datawaveQueryMetricFactory,
                        markingFunctions);
    }
    
    @Bean
    @Qualifier("queryMetrics")
    public TypeMetadataHelper.Factory typeMetadataFactory(ApplicationContext context) {
        return new TypeMetadataHelper.Factory(context);
    }
    
    @Bean
    @Qualifier("allMetadataAuths")
    public Set<Authorizations> allMetadataAuths(MetadataProperties metadataProperties) {
        Set<Authorizations> allMetadataAuths = new HashSet<>();
        metadataProperties.getAllMetadataAuths().forEach(a -> {
            allMetadataAuths.add(new Authorizations(a));
        });
        return allMetadataAuths;
    }
    
    @Bean
    @Qualifier("typeSubstitutions")
    public Map<String,String> typeSubstitutions(MetadataProperties metadataProperties) {
        return metadataProperties.getTypeSubstitutions();
    }
    
    @Bean
    public CompositeMetadataHelper compositeMetadataHelper(@Qualifier("warehouse") AccumuloConnectionPool connectionPool,
                    QueryMetricHandlerProperties queryMetricHandlerProperties, @Qualifier("allMetadataAuths") Set<Authorizations> allMetadataAuths)
                    throws Exception {
        Map<String,String> trackingMap = AccumuloConnectionTracking.getTrackingMap(Thread.currentThread().getStackTrace());
        Connector connector = connectionPool.borrowObject(trackingMap);
        return new CompositeMetadataHelper(connector, queryMetricHandlerProperties.getMetadataTableName(), allMetadataAuths);
    }
}
