package datawave.microservice.querymetric.handler;

import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.microservice.security.util.DnUtils;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.services.common.connection.AccumuloConnectionFactory.Priority;
import datawave.services.common.connection.AccumuloConnectionPool;
import datawave.services.query.logic.QueryLogic;
import datawave.services.query.logic.QueryLogicTransformer;
import datawave.webservice.query.Query;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.result.BaseQueryResponse;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static datawave.security.authorization.DatawaveUser.UserType.USER;

public class LocalShardTableQueryMetricHandler<T extends BaseQueryMetric> extends ShardTableQueryMetricHandler<T> {
    private static final Logger log = LoggerFactory.getLogger(LocalShardTableQueryMetricHandler.class);
    
    protected datawave.webservice.query.cache.QueryMetricFactory datawaveQueryMetricFactory;
    
    private final DatawavePrincipal datawavePrincipal;
    private final Map<String,CachedQuery> cachedQueryMap = new HashMap<>();
    
    public LocalShardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties,
                    @Qualifier("warehouse") AccumuloConnectionPool connectionPool, QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory,
                    datawave.webservice.query.cache.QueryMetricFactory datawaveQueryMetricFactory, MarkingFunctions markingFunctions, DnUtils dnUtils) {
        super(queryMetricHandlerProperties, connectionPool, logicFactory, metricFactory, markingFunctions, dnUtils);
        
        this.datawaveQueryMetricFactory = datawaveQueryMetricFactory;
        
        Collection<String> auths = new ArrayList<>();
        if (connectorAuthorizations != null) {
            auths.addAll(Arrays.asList(StringUtils.split(connectorAuthorizations, ',')));
        }
        DatawaveUser datawaveUser = new DatawaveUser(SubjectIssuerDNPair.of("admin"), USER, null, auths, null, null, System.currentTimeMillis());
        datawavePrincipal = new DatawavePrincipal(Collections.singletonList(datawaveUser));
    }
    
    @Override
    protected BaseQueryResponse createAndNext(Query query) throws Exception {
        String queryId = query.getId().toString();
        
        RunningQuery runningQuery;
        Connector connector;
        try {
            CachedQuery cachedQuery = new CachedQuery();
            cachedQueryMap.put(queryId, cachedQuery);
            
            QueryLogic<?> queryLogic = logicFactory.getObject();
            Map<String,String> trackingMap = AccumuloConnectionTracking.getTrackingMap(Thread.currentThread().getStackTrace());
            connector = connectionPool.borrowObject(trackingMap);
            
            cachedQuery.setConnector(connector);
            
            runningQuery = new RunningQuery(null, connector, Priority.ADMIN, queryLogic, query, query.getQueryAuthorizations(), datawavePrincipal,
                            this.datawaveQueryMetricFactory);
            
            cachedQuery.setRunningQuery(runningQuery);
            
            QueryLogicTransformer<?,?> transformer = queryLogic.getTransformer(query);
            cachedQuery.setTransformer(transformer);
            
            BaseQueryResponse response = transformer.createResponse(runningQuery.next());
            response.setQueryId(queryId);
            return response;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException("Running query create and next call failed", e);
        }
    }
    
    @Override
    protected BaseQueryResponse next(String queryId) throws Exception {
        try {
            CachedQuery cachedQuery = cachedQueryMap.get(queryId);
            return cachedQuery.getTransformer().createResponse(cachedQuery.getRunningQuery().next());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException("Running query next call failed", e);
        }
    }
    
    @Override
    protected void close(String queryId) {
        try {
            CachedQuery cachedQuery = cachedQueryMap.remove(queryId);
            if (cachedQuery.getConnector() != null) {
                this.connectionPool.returnObject(cachedQuery.getConnector());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException("Running query close call failed", e);
        }
    }
    
    private static class CachedQuery {
        private RunningQuery runningQuery;
        private QueryLogicTransformer<?,?> transformer;
        private Connector connector;
        
        public RunningQuery getRunningQuery() {
            return runningQuery;
        }
        
        public void setRunningQuery(RunningQuery runningQuery) {
            this.runningQuery = runningQuery;
        }
        
        public QueryLogicTransformer<?,?> getTransformer() {
            return transformer;
        }
        
        public void setTransformer(QueryLogicTransformer<?,?> transformer) {
            this.transformer = transformer;
        }
        
        public Connector getConnector() {
            return connector;
        }
        
        public void setConnector(Connector connector) {
            this.connector = connector;
        }
    }
}
