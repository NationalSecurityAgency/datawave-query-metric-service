package datawave.microservice.querymetric.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.microservice.querymetric.factory.BaseQueryMetricListResponseFactory;
import datawave.microservice.querymetric.factory.QueryMetricListResponseFactory;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.microservice.querymetric.handler.QueryGeometryHandler;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.handler.SimpleQueryGeometryHandler;
import datawave.webservice.common.connection.AccumuloConnectionPool;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({QueryMetricHandlerProperties.class, TimelyProperties.class})
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
    
    @Bean
    @ConditionalOnMissingBean
    QueryMetricFactory queryMetricFactory() {
        return new QueryMetricFactoryImpl();
    }
    
    @Bean
    @ConditionalOnMissingBean
    public ShardTableQueryMetricHandler shardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties,
                    @Qualifier("warehouse") AccumuloConnectionPool connectionPool, QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory,
                    MarkingFunctions markingFunctions) {
        return new ShardTableQueryMetricHandler(queryMetricHandlerProperties, connectionPool, logicFactory, metricFactory, markingFunctions);
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
}
