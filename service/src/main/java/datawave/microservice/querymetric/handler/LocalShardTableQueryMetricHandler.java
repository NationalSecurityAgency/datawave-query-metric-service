package datawave.microservice.querymetric.handler;

import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.microservice.security.util.DnUtils;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static datawave.security.authorization.DatawaveUser.UserType.USER;

public class LocalShardTableQueryMetricHandler<T extends BaseQueryMetric> extends ShardTableQueryMetricHandler<T> {
    private static final Logger log = LoggerFactory.getLogger(LocalShardTableQueryMetricHandler.class);
    
    protected final datawave.microservice.querymetric.QueryMetricFactory datawaveQueryMetricFactory;
    
    private final DatawavePrincipal datawavePrincipal;
    private final Map<String,CachedQuery> cachedQueryMap = new HashMap<>();
    
    public LocalShardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties,
                    @Qualifier("warehouse") AccumuloConnectionPool connectionPool, QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory,
                    MarkingFunctions markingFunctions, QueryMetricCombiner queryMetricCombiner, LuceneToJexlQueryParser luceneToJexlQueryParser,
                    DnUtils dnUtils) {
        super(queryMetricHandlerProperties, connectionPool, logicFactory, metricFactory, markingFunctions, queryMetricCombiner, luceneToJexlQueryParser,
                        dnUtils);
        
        this.datawaveQueryMetricFactory = metricFactory;
        
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
        
        Future<BaseQueryResponse> createAndNextFuture = null;
        final CachedQuery cachedQuery = new CachedQuery();
        try {
            createAndNextFuture = cachedQuery.getExecutor().submit(() -> {
                RunningQuery runningQuery;
                Connector connector;
                
                cachedQueryMap.put(queryId, cachedQuery);
                
                QueryLogic<?> queryLogic = logicFactory.getObject();
                Map<String,String> trackingMap = AccumuloConnectionTracking.getTrackingMap(Thread.currentThread().getStackTrace());
                connector = connectionPool.borrowObject(trackingMap);
                
                cachedQuery.setConnector(connector);
                
                runningQuery = new RunningQuery(null, connector, Priority.ADMIN, queryLogic, query, query.getQueryAuthorizations(), datawavePrincipal,
                                datawaveQueryMetricFactory);
                
                cachedQuery.setRunningQuery(runningQuery);
                
                QueryLogicTransformer<?,?> transformer = queryLogic.getTransformer(query);
                cachedQuery.setTransformer(transformer);
                
                BaseQueryResponse response = transformer.createResponse(runningQuery.next());
                response.setQueryId(queryId);
                return response;
            });
            
            return createAndNextFuture.get(
                            Math.max(0, queryMetricHandlerProperties.getMaxReadMilliseconds() - (System.currentTimeMillis() - cachedQuery.getStartTime())),
                            TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            log.error(e.getMessage(), e);
            // unwrap the execution exception
            throw new IllegalStateException("Running query create and next call failed", e.getCause());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException("Running query create and next call failed", e);
        } finally {
            if (createAndNextFuture != null) {
                createAndNextFuture.cancel(true);
            }
        }
    }
    
    @Override
    protected BaseQueryResponse next(String queryId) throws Exception {
        Future<BaseQueryResponse> nextFuture = null;
        final CachedQuery cachedQuery = cachedQueryMap.get(queryId);
        try {
            nextFuture = cachedQuery.getExecutor().submit(() -> cachedQuery.getTransformer().createResponse(cachedQuery.getRunningQuery().next()));
            
            return nextFuture.get(
                            Math.max(0, queryMetricHandlerProperties.getMaxReadMilliseconds() - (System.currentTimeMillis() - cachedQuery.getStartTime())),
                            TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            log.error(e.getMessage(), e);
            // unwrap the execution exception
            throw new IllegalStateException("Running query next call failed", e.getCause());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException("Running query next call failed", e);
        } finally {
            if (nextFuture != null) {
                nextFuture.cancel(true);
            }
        }
    }
    
    @Override
    protected void close(String queryId) {
        try {
            CachedQuery cachedQuery = cachedQueryMap.remove(queryId);
            if (cachedQuery.getConnector() != null) {
                this.connectionPool.returnObject(cachedQuery.getConnector());
            }
            if (cachedQuery.getExecutor() != null) {
                cachedQuery.getExecutor().shutdownNow();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException("Running query close call failed", e);
        }
    }
    
    private static class CachedQuery {
        private long startTime = System.currentTimeMillis();
        
        private ExecutorService executor = Executors.newSingleThreadExecutor();
        
        private RunningQuery runningQuery;
        private QueryLogicTransformer<?,?> transformer;
        private Connector connector;
        
        public long getStartTime() {
            return startTime;
        }
        
        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }
        
        public ExecutorService getExecutor() {
            return executor;
        }
        
        public void setExecutor(ExecutorService executor) {
            this.executor = executor;
        }
        
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
