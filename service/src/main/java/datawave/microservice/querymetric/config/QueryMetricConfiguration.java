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
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.microservice.querymetric.QueryMetricOperations;
import datawave.microservice.querymetric.factory.BaseQueryMetricListResponseFactory;
import datawave.microservice.querymetric.factory.QueryMetricListResponseFactory;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.microservice.querymetric.function.QueryMetricConsumer;
import datawave.microservice.querymetric.handler.LocalShardTableQueryMetricHandler;
import datawave.microservice.querymetric.handler.QueryGeometryHandler;
import datawave.microservice.querymetric.handler.RemoteShardTableQueryMetricHandler;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.handler.SimpleQueryGeometryHandler;
import datawave.microservice.security.util.DnUtils;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.TypeMetadataHelper;
import datawave.security.authorization.JWTTokenHandler;
import datawave.services.common.connection.AccumuloConnectionPool;
import datawave.services.query.result.event.DefaultResponseObjectFactory;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

import static datawave.query.util.MetadataHelperFactory.ALL_AUTHS_PROPERTY;

@Configuration
@EnableConfigurationProperties({QueryMetricHandlerProperties.class, TimelyProperties.class})
public class QueryMetricConfiguration {
    
    @Bean
    public QueryMetricConsumer queryMetricSink(QueryMetricOperations queryMetricOperations) {
        return new QueryMetricConsumer(queryMetricOperations);
    }
    
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
    
    @Bean
    @ConditionalOnMissingBean
    QueryMetricFactory queryMetricFactory() {
        return new QueryMetricFactoryImpl();
    }
    
    @Bean
    @ConditionalOnMissingBean
    public ShardTableQueryMetricHandler shardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties,
                    @Qualifier("warehouse") AccumuloConnectionPool connectionPool, QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory,
                    MarkingFunctions markingFunctions, WebClient.Builder webClientBuilder, @Autowired(required = false) JWTTokenHandler jwtTokenHandler,
                    DnUtils dnUtils) {
        if (queryMetricHandlerProperties.isUseRemoteQuery()) {
            return new RemoteShardTableQueryMetricHandler(queryMetricHandlerProperties, connectionPool, logicFactory, metricFactory, markingFunctions,
                            webClientBuilder, jwtTokenHandler, dnUtils);
        } else {
            return new LocalShardTableQueryMetricHandler(queryMetricHandlerProperties, connectionPool, logicFactory, metricFactory, markingFunctions, dnUtils);
        }
    }
    
    @Bean
    @ConditionalOnMissingBean
    public QueryGeometryHandler geometryHandler(QueryMetricHandlerProperties queryMetricHandlerProperties) {
        return new SimpleQueryGeometryHandler(queryMetricHandlerProperties);
    }
    
    @Bean
    public BaseQueryMetricListResponseFactory queryMetricListResponseFactory() {
        return new QueryMetricListResponseFactory();
    }
    
    // This bean is used via autowire in DateIndexHelper
    @Bean(name = "dateIndexHelperCacheManager")
    public CaffeineCacheManager dateIndexHelperCacheManager(QueryMetricHandlerProperties queryMetricHandlerProperties) {
        System.setProperty(ALL_AUTHS_PROPERTY, queryMetricHandlerProperties.getMetadataDefaultAuths());
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeineSpec(CaffeineSpec.parse("maximumSize=1000, expireAfterAccess=24h, expireAfterWrite=24h"));
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
    @Qualifier("queryMetrics")
    public TypeMetadataHelper.Factory typeMetadataFactory(ApplicationContext context) {
        return new TypeMetadataHelper.Factory(context);
    }
}
