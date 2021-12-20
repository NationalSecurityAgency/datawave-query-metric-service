package datawave.microservice.querymetric.handler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import datawave.data.hash.UID;
import datawave.data.hash.UIDBuilder;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.LiveContextWriter;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.marking.MarkingFunctions;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Lifecycle;
import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.microservice.querymetric.QueryMetricsSummaryResponse;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.query.iterator.QueryOptions;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.AuthorizationsUtil;
import datawave.security.util.DnUtils;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.common.connection.AccumuloConnectionPool;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.query.util.QueryUtil;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.EventQueryResponseBase;
import datawave.webservice.result.VoidResponse;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static datawave.security.authorization.DatawaveUser.UserType.USER;

public class ShardTableQueryMetricHandler<T extends BaseQueryMetric> extends BaseQueryMetricHandler<T> {
    private static final Logger log = ThreadConfigurableLogger.getLogger(ShardTableQueryMetricHandler.class.getName());
    
    protected static final String QUERY_METRICS_LOGIC_NAME = "QueryMetricsQuery";
    protected String connectorAuthorizations = null;
    
    private AccumuloConnectionPool connectionPool;
    private QueryMetricHandlerProperties queryMetricHandlerProperties;
    
    @SuppressWarnings("FieldCanBeLocal")
    private final String JOB_ID = "job_201109071404_1";
    
    private final Configuration conf = new Configuration();
    private final StatusReporter reporter = new MockStatusReporter();
    private final AtomicBoolean tablesChecked = new AtomicBoolean(false);
    private AccumuloRecordWriter recordWriter = null;
    private QueryMetricQueryLogicFactory logicFactory;
    private QueryMetricFactory metricFactory;
    private datawave.webservice.query.cache.QueryMetricFactory datawaveQueryMetricFactory;
    private UIDBuilder<UID> uidBuilder = UID.builder();
    private DatawavePrincipal datawavePrincipal;
    private MarkingFunctions markingFunctions;
    
    public ShardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties,
                    @Qualifier("warehouse") AccumuloConnectionPool connectionPool, QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory,
                    datawave.webservice.query.cache.QueryMetricFactory datawaveQueryMetricFactory, MarkingFunctions markingFunctions) {
        this.queryMetricHandlerProperties = queryMetricHandlerProperties;
        this.logicFactory = logicFactory;
        this.metricFactory = metricFactory;
        this.datawaveQueryMetricFactory = datawaveQueryMetricFactory;
        this.markingFunctions = markingFunctions;
        this.connectionPool = connectionPool;
        queryMetricHandlerProperties.getProperties().entrySet().forEach(e -> conf.set(e.getKey(), e.getValue()));
        
        Connector connector = null;
        try {
            log.info("creating connector with username:" + queryMetricHandlerProperties.getUsername() + " password:"
                            + queryMetricHandlerProperties.getPassword());
            Map<String,String> trackingMap = AccumuloConnectionTracking.getTrackingMap(Thread.currentThread().getStackTrace());
            connector = connectionPool.borrowObject(trackingMap);
            connectorAuthorizations = connector.securityOperations().getUserAuthorizations(connector.whoami()).toString();
            reload();
            
            if (tablesChecked.compareAndSet(false, true)) {
                verifyTables();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (connector != null) {
                this.connectionPool.returnObject(connector);
            }
        }
        Collection<String> auths = new ArrayList<>();
        if (connectorAuthorizations != null) {
            Arrays.stream(StringUtils.split(connectorAuthorizations, ',')).forEach(a -> {
                auths.add(a);
            });
        }
        DatawaveUser datawaveUser = new DatawaveUser(SubjectIssuerDNPair.of("admin"), USER, null, auths, null, null, System.currentTimeMillis());
        datawavePrincipal = new DatawavePrincipal(Collections.singletonList(datawaveUser));
    }
    
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.recordWriter.close(null);
    }
    
    @Override
    public void flush() throws Exception {
        this.recordWriter.flush();
    }
    
    public void verifyTables() {
        Connector connector = null;
        try {
            AbstractColumnBasedHandler<Key> handler = new ContentIndexingColumnBasedHandler() {
                @Override
                public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
                    return getQueryMetricsIngestHelper(false);
                }
            };
            Map<String,String> trackingMap = AccumuloConnectionTracking.getTrackingMap(Thread.currentThread().getStackTrace());
            connector = connectionPool.borrowObject(trackingMap);
            createAndConfigureTablesIfNecessary(handler.getTableNames(conf), connector.tableOperations(), conf);
        } catch (Exception e) {
            log.error("Error verifying table configuration", e);
        } finally {
            if (connector != null) {
                this.connectionPool.returnObject(connector);
            }
        }
    }
    
    public void writeMetric(T updatedQueryMetric, List<T> storedQueryMetrics, Date lastUpdated, boolean delete) throws Exception {
        LiveContextWriter contextWriter = null;
        MapContext<Text,RawRecordContainer,Text,Mutation> context = null;
        
        try {
            contextWriter = new LiveContextWriter();
            contextWriter.setup(conf, false);
            
            TaskAttemptID taskId = new TaskAttemptID(new TaskID(new JobID(JOB_ID, 1), TaskType.MAP, 1), 1);
            context = new MapContextImpl<>(conf, taskId, null, recordWriter, null, reporter, null);
            
            for (T storedQueryMetric : storedQueryMetrics) {
                ContentIndexingColumnBasedHandler handler = new ContentIndexingColumnBasedHandler() {
                    @Override
                    public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
                        return getQueryMetricsIngestHelper(delete);
                    }
                };
                handler.setup(context);
                
                Multimap<BulkIngestKey,Value> r = getEntries(handler, updatedQueryMetric, storedQueryMetric, lastUpdated);
                
                try {
                    if (r != null) {
                        contextWriter.write(r, context);
                    }
                    
                    if (!delete && handler.getMetadata() != null) {
                        contextWriter.write(handler.getMetadata().getBulkMetadata(), context);
                    }
                } finally {
                    contextWriter.commit(context);
                }
            }
        } finally {
            if (contextWriter != null && context != null) {
                contextWriter.cleanup(context);
            }
        }
    }
    
    public Map<String,String> getEventFields(BaseQueryMetric queryMetric) {
        // ignore duplicates as none are expected
        Map<String,String> eventFields = new HashMap<>();
        ContentQueryMetricsIngestHelper ingestHelper = getQueryMetricsIngestHelper(false);
        ingestHelper.setup(conf);
        Multimap<String,NormalizedContentInterface> fieldsToWrite = ingestHelper.getEventFieldsToWrite(queryMetric);
        for (Entry<String,NormalizedContentInterface> entry : fieldsToWrite.entries()) {
            eventFields.put(entry.getKey(), entry.getValue().getEventFieldValue());
        }
        return eventFields;
    }
    
    private Multimap<BulkIngestKey,Value> getEntries(ContentIndexingColumnBasedHandler handler, T updatedQueryMetric, T storedQueryMetric, Date lastUpdated) {
        Type type = TypeRegistry.getType("querymetrics");
        ContentQueryMetricsIngestHelper ingestHelper = (ContentQueryMetricsIngestHelper) handler.getContentIndexingDataTypeHelper();
        ingestHelper.setup(conf);
        boolean deleteMode = ingestHelper.getDeleteMode();
        
        RawRecordContainerImpl event = new RawRecordContainerImpl();
        event.setConf(this.conf);
        event.setDataType(type);
        event.setDate(storedQueryMetric.getCreateDate().getTime());
        // get markings from metric, otherwise use the default markings
        Map<String,String> markings = updatedQueryMetric.getMarkings();
        if (markings != null && !markings.isEmpty()) {
            try {
                event.setVisibility(this.markingFunctions.translateToColumnVisibility(updatedQueryMetric.getMarkings()));
            } catch (MarkingFunctions.Exception e) {
                log.error(e.getMessage(), e);
                event.setVisibility(this.queryMetricHandlerProperties.getDefaultMetricVisibility());
            }
        } else {
            event.setVisibility(this.queryMetricHandlerProperties.getDefaultMetricVisibility());
        }
        event.setAuxData(storedQueryMetric);
        event.setRawRecordNumber(1000L);
        event.addAltId(storedQueryMetric.getQueryId());
        
        event.setId(uidBuilder.newId(storedQueryMetric.getQueryId().getBytes(Charset.forName("UTF-8")), (Date) null));
        
        final Multimap<String,NormalizedContentInterface> fields;
        
        if (deleteMode) {
            fields = ingestHelper.getEventFieldsToDelete(updatedQueryMetric, storedQueryMetric);
        } else {
            fields = ingestHelper.getEventFieldsToWrite(updatedQueryMetric);
        }
        
        Key key = new Key();
        
        if (handler.getMetadata() != null) {
            handler.getMetadata().addEventWithoutLoadDates(ingestHelper, event, fields);
        }
        
        String indexTable = handler.getShardIndexTableName().toString();
        String reverseIndexTable = handler.getShardReverseIndexTableName().toString();
        int fieldSizeThreshold = ingestHelper.getFieldSizeThreshold();
        Multimap<BulkIngestKey,Value> r = handler.processBulk(key, event, fields, reporter);
        List<BulkIngestKey> keysToRemove = new ArrayList<>();
        Map<String,BulkIngestKey> tfFields = new HashMap<>();
        
        // if an event has more than two entries for a given field, only keep the longest
        for (Entry<BulkIngestKey,Collection<Value>> entry : r.asMap().entrySet()) {
            String table = entry.getKey().getTableName().toString();
            BulkIngestKey bulkIngestKey = entry.getKey();
            Key currentKey = bulkIngestKey.getKey();
            
            if (table.equals(indexTable) || table.equals(reverseIndexTable)) {
                String value = currentKey.getRow().toString();
                if (value.length() > fieldSizeThreshold) {
                    keysToRemove.add(bulkIngestKey);
                }
            }
        }
        
        // remove any keys from the index or reverseIndex where the value size exceeds the fieldSizeThreshold
        for (BulkIngestKey b : keysToRemove) {
            r.removeAll(b);
        }
        
        // replace the longest of the keys from fields that get parse
        // d as content
        for (Entry<String,BulkIngestKey> l : tfFields.entrySet()) {
            r.put(l.getValue(), new Value(new byte[0]));
        }
        
        for (Entry<BulkIngestKey,Collection<Value>> entry : r.asMap().entrySet()) {
            if (deleteMode) {
                entry.getKey().getKey().setTimestamp(lastUpdated.getTime());
            } else {
                // this will ensure that the QueryMetrics can be found within second precision in most cases
                entry.getKey().getKey().setTimestamp(storedQueryMetric.getCreateDate().getTime() + storedQueryMetric.getNumUpdates());
            }
            entry.getKey().getKey().setDeleted(deleteMode);
        }
        
        return r;
    }
    
    public void setIfDifferent(String current, String updated) {
        if (updated != null) {
            if (current == null) {
                
            }
        }
    }
    
    public long getLastPageNumber(BaseQueryMetric m) {
        long lastPage = 0;
        List<BaseQueryMetric.PageMetric> pageMetrics = m.getPageTimes();
        for (BaseQueryMetric.PageMetric pm : pageMetrics) {
            if (lastPage == 0 || pm.getPageNumber() > lastPage) {
                lastPage = pm.getPageNumber();
            }
        }
        return lastPage;
    }
    
    private PageMetric combinePageMetrics(PageMetric updated, PageMetric stored) {
        if (stored == null) {
            return updated;
        }
        String updatedUuid = updated.getPageUuid();
        String storedUuid = stored.getPageUuid();
        if (updatedUuid != null && storedUuid != null && !updatedUuid.equals(storedUuid)) {
            throw new IllegalStateException(
                            "can not combine page metrics with different pageUuids: " + "updated:" + updated.getPageUuid() + " stored:" + stored.getPageUuid());
        }
        PageMetric pm = new PageMetric(stored);
        if (pm.getHost() == null) {
            pm.setHost(updated.getHost());
        }
        if (pm.getPagesize() == 0) {
            pm.setPagesize(updated.getPagesize());
        }
        if (pm.getReturnTime() == -1) {
            pm.setReturnTime(updated.getReturnTime());
        }
        if (pm.getCallTime() == -1) {
            pm.setCallTime(updated.getCallTime());
        }
        if (pm.getSerializationTime() == -1) {
            pm.setSerializationTime(updated.getSerializationTime());
        }
        if (pm.getBytesWritten() == -1) {
            pm.setBytesWritten(updated.getBytesWritten());
        }
        if (pm.getPageRequested() == 0) {
            pm.setPageRequested(updated.getPageRequested());
        }
        if (pm.getPageReturned() == 0) {
            pm.setPageReturned(updated.getPageReturned());
        }
        if (pm.getLoginTime() == -1) {
            pm.setLoginTime(updated.getLoginTime());
        }
        return pm;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public T combineMetrics(T updatedQueryMetric, T cachedQueryMetric, QueryMetricType metricType) throws Exception {
        
        // new metrics coming in may be complete or partial updates
        if (cachedQueryMetric != null) {
            // only update once
            if (cachedQueryMetric.getQueryType() == null && updatedQueryMetric.getQueryType() != null) {
                cachedQueryMetric.setQueryType(updatedQueryMetric.getQueryType());
            }
            // only update once
            if (cachedQueryMetric.getUser() == null && updatedQueryMetric.getUser() != null) {
                cachedQueryMetric.setUser(updatedQueryMetric.getUser());
            }
            // only update once
            if (cachedQueryMetric.getUserDN() == null && updatedQueryMetric.getUserDN() != null) {
                cachedQueryMetric.setUserDN(updatedQueryMetric.getUserDN());
            }
            // keep the earliest create date
            long cachedCreate = cachedQueryMetric.getCreateDate() == null ? Long.MAX_VALUE : cachedQueryMetric.getCreateDate().getTime();
            long updatedCreate = updatedQueryMetric.getCreateDate() == null ? Long.MAX_VALUE : updatedQueryMetric.getCreateDate().getTime();
            if (updatedCreate < cachedCreate) {
                cachedQueryMetric.setCreateDate(updatedQueryMetric.getCreateDate());
            }
            
            // Do not update queryId -- shouldn't change anyway
            
            // only update once
            if (cachedQueryMetric.getQuery() == null && updatedQueryMetric.getQuery() != null) {
                cachedQueryMetric.setQuery(updatedQueryMetric.getQuery());
            }
            // only update once
            if (cachedQueryMetric.getHost() == null && updatedQueryMetric.getHost() != null) {
                cachedQueryMetric.setHost(updatedQueryMetric.getHost());
            }
            
            // Map page numbers to page metrics and update
            Map<Long,PageMetric> storedPagesByPageNumMap = new TreeMap<>();
            Map<String,PageMetric> storedPagesByUuidMap = new TreeMap<>();
            if (cachedQueryMetric.getPageTimes() != null) {
                cachedQueryMetric.getPageTimes().forEach(pm -> {
                    storedPagesByPageNumMap.put(pm.getPageNumber(), pm);
                    if (pm.getPageUuid() != null) {
                        storedPagesByUuidMap.put(pm.getPageUuid(), pm);
                    }
                });
            }
            // combine all of the page metrics from the cached metric and the updated metric
            if (updatedQueryMetric.getPageTimes() != null) {
                long pageNum = getLastPageNumber(cachedQueryMetric) + 1;
                for (PageMetric updatedPage : updatedQueryMetric.getPageTimes()) {
                    PageMetric storedPage = null;
                    if (updatedPage.getPageUuid() != null) {
                        storedPage = storedPagesByUuidMap.get(updatedPage.getPageUuid());
                    }
                    if (metricType.equals(QueryMetricType.DISTRIBUTED)) {
                        if (storedPage != null) {
                            // updatedPage found by pageUuid
                            updatedPage = combinePageMetrics(updatedPage, storedPage);
                            storedPagesByPageNumMap.put(updatedPage.getPageNumber(), updatedPage);
                        } else {
                            // assume that this is the next page in sequence
                            updatedPage.setPageNumber(pageNum);
                            storedPagesByPageNumMap.put(pageNum, updatedPage);
                            pageNum++;
                        }
                    } else {
                        if (storedPage == null) {
                            storedPage = storedPagesByPageNumMap.get(updatedPage.getPageNumber());
                        }
                        if (storedPage != null) {
                            updatedPage = combinePageMetrics(updatedPage, storedPage);
                        }
                        // page metrics are mapped to their page number to prevent duplicates
                        storedPagesByPageNumMap.put(updatedPage.getPageNumber(), updatedPage);
                    }
                }
            }
            cachedQueryMetric.setPageTimes(new ArrayList<>(storedPagesByPageNumMap.values()));
            cachedQueryMetric.setNumUpdates(cachedQueryMetric.getNumUpdates() + 1);
            
            // only update once
            if (cachedQueryMetric.getProxyServers() == null && updatedQueryMetric.getProxyServers() != null) {
                cachedQueryMetric.setProxyServers(updatedQueryMetric.getProxyServers());
            }
            // only update once
            if (cachedQueryMetric.getErrorMessage() == null && updatedQueryMetric.getErrorMessage() != null) {
                cachedQueryMetric.setErrorMessage(updatedQueryMetric.getErrorMessage());
            }
            // only update once
            if (cachedQueryMetric.getErrorCode() == null && updatedQueryMetric.getErrorCode() != null) {
                cachedQueryMetric.setErrorCode(updatedQueryMetric.getErrorCode());
            }
            // use updated lifecycle unless trying to update a final lifecycle with a non-final lifecycle
            if ((cachedQueryMetric.isLifecycleFinal() && !updatedQueryMetric.isLifecycleFinal()) == false) {
                cachedQueryMetric.setLifecycle(updatedQueryMetric.getLifecycle());
            }
            // only update once
            if (cachedQueryMetric.getQueryAuthorizations() == null && updatedQueryMetric.getQueryAuthorizations() != null) {
                cachedQueryMetric.setQueryAuthorizations(updatedQueryMetric.getQueryAuthorizations());
            }
            // only update once
            if (cachedQueryMetric.getBeginDate() == null && updatedQueryMetric.getBeginDate() != null) {
                cachedQueryMetric.setBeginDate(updatedQueryMetric.getBeginDate());
            }
            // only update once
            if (cachedQueryMetric.getEndDate() == null && updatedQueryMetric.getEndDate() != null) {
                cachedQueryMetric.setEndDate(updatedQueryMetric.getEndDate());
            }
            // only update once
            if (cachedQueryMetric.getPositiveSelectors() == null && updatedQueryMetric.getPositiveSelectors() != null) {
                cachedQueryMetric.setPositiveSelectors(updatedQueryMetric.getPositiveSelectors());
            }
            // only update once
            if (cachedQueryMetric.getNegativeSelectors() == null && updatedQueryMetric.getNegativeSelectors() != null) {
                cachedQueryMetric.setNegativeSelectors(updatedQueryMetric.getNegativeSelectors());
            }
            if (updatedQueryMetric.getLastUpdated() != null) {
                // keep the latest last updated date
                if (cachedQueryMetric.getLastUpdated() == null
                                || (updatedQueryMetric.getLastUpdated().getTime() > cachedQueryMetric.getLastUpdated().getTime())) {
                    cachedQueryMetric.setLastUpdated(updatedQueryMetric.getLastUpdated());
                }
            }
            // only update once
            if (cachedQueryMetric.getColumnVisibility() == null && updatedQueryMetric.getColumnVisibility() != null) {
                cachedQueryMetric.setColumnVisibility(updatedQueryMetric.getColumnVisibility());
            }
            // only update once
            if (cachedQueryMetric.getQueryLogic() == null && updatedQueryMetric.getQueryLogic() != null) {
                cachedQueryMetric.setQueryLogic(updatedQueryMetric.getQueryLogic());
            }
            // only update once
            if (cachedQueryMetric.getQueryName() == null && updatedQueryMetric.getQueryName() != null) {
                cachedQueryMetric.setQueryName(updatedQueryMetric.getQueryName());
            }
            // only update once
            if (cachedQueryMetric.getParameters() == null && updatedQueryMetric.getParameters() != null) {
                cachedQueryMetric.setParameters(updatedQueryMetric.getParameters());
            }
            // only update once
            if (cachedQueryMetric.getSetupTime() > -1) {
                cachedQueryMetric.setSetupTime(updatedQueryMetric.getSetupTime());
            }
            // only update once
            if (cachedQueryMetric.getCreateCallTime() > -1) {
                cachedQueryMetric.setCreateCallTime(updatedQueryMetric.getCreateCallTime());
            }
            // only update once
            if (cachedQueryMetric.getLoginTime() > -1) {
                cachedQueryMetric.setLoginTime(updatedQueryMetric.getLoginTime());
            }
            
            if (metricType.equals(QueryMetricType.DISTRIBUTED)) {
                cachedQueryMetric.setSourceCount(cachedQueryMetric.getSourceCount() + updatedQueryMetric.getSourceCount());
                cachedQueryMetric.setNextCount(cachedQueryMetric.getNextCount() + updatedQueryMetric.getNextCount());
                cachedQueryMetric.setSeekCount(cachedQueryMetric.getSeekCount() + updatedQueryMetric.getSeekCount());
                cachedQueryMetric.setYieldCount(cachedQueryMetric.getYieldCount() + updatedQueryMetric.getYieldCount());
                cachedQueryMetric.setDocRanges(cachedQueryMetric.getDocRanges() + updatedQueryMetric.getDocRanges());
                cachedQueryMetric.setFiRanges(cachedQueryMetric.getFiRanges() + updatedQueryMetric.getFiRanges());
            } else {
                cachedQueryMetric.setSourceCount(updatedQueryMetric.getSourceCount());
                cachedQueryMetric.setNextCount(updatedQueryMetric.getNextCount());
                cachedQueryMetric.setSeekCount(updatedQueryMetric.getSeekCount());
                cachedQueryMetric.setYieldCount(updatedQueryMetric.getYieldCount());
                cachedQueryMetric.setDocRanges(updatedQueryMetric.getDocRanges());
                cachedQueryMetric.setFiRanges(updatedQueryMetric.getFiRanges());
            }
            // only update once
            if (cachedQueryMetric.getPlan() == null && updatedQueryMetric.getPlan() != null) {
                cachedQueryMetric.setPlan(updatedQueryMetric.getPlan());
            }
            // only update once
            if (cachedQueryMetric.getPredictions() == null && updatedQueryMetric.getPredictions() != null) {
                cachedQueryMetric.setPredictions(updatedQueryMetric.getPredictions());
            }
        }
        return cachedQueryMetric;
    }
    
    public T getQueryMetric(final String queryId) {
        List<T> queryMetrics;
        VoidResponse response = new VoidResponse();
        queryMetrics = getQueryMetrics(response, "QUERY_ID == '" + queryId + "'");
        List<QueryExceptionType> exceptions = response.getExceptions();
        if (exceptions != null && !exceptions.isEmpty()) {
            exceptions.forEach(e -> {
                log.error(e.getMessage());
            });
        }
        return queryMetrics.isEmpty() ? null : queryMetrics.get(0);
    }
    
    public Query createQuery() {
        return new QueryImpl();
    }
    
    public List<T> getQueryMetrics(BaseResponse response, final String query) {
        Date end = new Date();
        Date begin = DateUtils.setYears(end, 2000);
        Query queryImpl = createQuery();
        queryImpl.setBeginDate(begin);
        queryImpl.setEndDate(end);
        queryImpl.setQueryLogicName(QUERY_METRICS_LOGIC_NAME);
        queryImpl.setQuery(query);
        queryImpl.setQueryName(QUERY_METRICS_LOGIC_NAME);
        queryImpl.setColumnVisibility(queryMetricHandlerProperties.getQueryVisibility());
        queryImpl.setQueryAuthorizations(this.connectorAuthorizations);
        queryImpl.setExpirationDate(DateUtils.addDays(new Date(), 1));
        queryImpl.setPagesize(1000);
        queryImpl.setId(UUID.randomUUID());
        queryImpl.setParameters(ImmutableMap.of(QueryOptions.INCLUDE_GROUPING_CONTEXT, "true"));
        return getQueryMetrics(response, queryImpl);
    }
    
    public List<T> getQueryMetrics(BaseResponse response, Query query) {
        List<T> queryMetrics = new ArrayList<>();
        RunningQuery runningQuery;
        
        Connector connector = null;
        try {
            QueryLogic<?> queryLogic = logicFactory.getObject();
            Map<String,String> trackingMap = AccumuloConnectionTracking.getTrackingMap(Thread.currentThread().getStackTrace());
            connector = connectionPool.borrowObject(trackingMap);
            runningQuery = new RunningQuery(null, connector, Priority.ADMIN, queryLogic, query, query.getQueryAuthorizations(), datawavePrincipal,
                            this.datawaveQueryMetricFactory);
            
            boolean done = false;
            List<Object> objectList = new ArrayList<>();
            
            while (!done) {
                ResultsPage resultsPage = runningQuery.next();
                
                if (!resultsPage.getResults().isEmpty()) {
                    objectList.addAll(resultsPage.getResults());
                } else {
                    done = true;
                }
            }
            
            BaseQueryResponse queryResponse = queryLogic.getTransformer(query).createResponse(new ResultsPage(objectList));
            List<QueryExceptionType> exceptions = queryResponse.getExceptions();
            
            if (queryResponse.getExceptions() != null && !queryResponse.getExceptions().isEmpty()) {
                if (response != null) {
                    response.setExceptions(new LinkedList<>(exceptions));
                    response.setHasResults(false);
                }
            }
            
            if (!(queryResponse instanceof EventQueryResponseBase)) {
                if (response != null) {
                    response.addException(new QueryException("incompatible response")); // TODO: Should this be an IllegalStateException?
                    response.setHasResults(false);
                }
            }
            
            EventQueryResponseBase eventQueryResponse = (EventQueryResponseBase) queryResponse;
            List<EventBase> eventList = eventQueryResponse.getEvents();
            
            for (EventBase<?,?> event : eventList) {
                T metric = toMetric(event);
                queryMetrics.add(metric);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            if (response != null) {
                response.addExceptions(new QueryException(e).getQueryExceptionsInStack());
            }
        } finally {
            if (connector != null) {
                this.connectionPool.returnObject(connector);
            }
        }
        return queryMetrics;
    }
    
    public T toMetric(EventBase event) {
        SimpleDateFormat sdf_date_time1 = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat sdf_date_time2 = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat sdf_date_time3 = new SimpleDateFormat("yyyyMMdd");
        
        List<String> excludedFields = Arrays.asList("ELAPSED_TIME", "RECORD_ID", "NUM_PAGES", "NUM_RESULTS");
        
        try {
            T m = (T) metricFactory.createMetric();
            List<FieldBase> field = event.getFields();
            m.setMarkings(event.getMarkings());
            TreeMap<Long,PageMetric> pageMetrics = Maps.newTreeMap();
            
            boolean createDateSet = false;
            for (FieldBase f : field) {
                String fieldName = f.getName();
                String fieldValue = f.getValueString();
                if (!excludedFields.contains(fieldName)) {
                    
                    if (fieldName.equals("USER")) {
                        m.setUser(fieldValue);
                    } else if (fieldName.equals("USER_DN")) {
                        m.setUserDN(fieldValue);
                    } else if (fieldName.equals("QUERY_ID")) {
                        m.setQueryId(fieldValue);
                    } else if (fieldName.equals("CREATE_DATE")) {
                        try {
                            Date d = sdf_date_time2.parse(fieldValue);
                            m.setCreateDate(d);
                            createDateSet = true;
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else if (fieldName.equals("QUERY")) {
                        m.setQuery(fieldValue);
                    } else if (fieldName.equals("PLAN")) {
                        m.setPlan(fieldValue);
                    } else if (fieldName.equals("QUERY_LOGIC")) {
                        m.setQueryLogic(fieldValue);
                    } else if (fieldName.equals("QUERY_ID")) {
                        m.setQueryId(fieldValue);
                    } else if (fieldName.equals("BEGIN_DATE")) {
                        try {
                            Date d = sdf_date_time1.parse(fieldValue);
                            m.setBeginDate(d);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else if (fieldName.equals("END_DATE")) {
                        try {
                            Date d = sdf_date_time1.parse(fieldValue);
                            m.setEndDate(d);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else if (fieldName.equals("HOST")) {
                        m.setHost(fieldValue);
                    } else if (fieldName.equals("PROXY_SERVERS")) {
                        m.setProxyServers(Arrays.asList(StringUtils.split(fieldValue, ",")));
                    } else if (fieldName.equals("AUTHORIZATIONS")) {
                        m.setQueryAuthorizations(fieldValue);
                    } else if (fieldName.equals("QUERY_TYPE")) {
                        m.setQueryType(fieldValue);
                    } else if (fieldName.equals("LIFECYCLE")) {
                        m.setLifecycle(Lifecycle.valueOf(fieldValue));
                    } else if (fieldName.equals("ERROR_CODE")) {
                        m.setErrorCode(fieldValue);
                    } else if (fieldName.equals("ERROR_MESSAGE")) {
                        m.setErrorMessage(fieldValue);
                    } else if (fieldName.equals("SETUP_TIME")) {
                        m.setSetupTime(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("CREATE_CALL_TIME")) {
                        m.setCreateCallTime(Long.parseLong(fieldValue));
                    } else if (fieldName.startsWith("PAGE_METRICS")) {
                        int index = fieldName.indexOf(".");
                        if (-1 == index) {
                            log.error("Could not parse field name to extract repetition count: " + fieldName);
                        } else {
                            Long pageNum = Long.parseLong(fieldName.substring(index + 1));
                            PageMetric pageMetric = PageMetric.parse(fieldValue);
                            if (pageMetric != null) {
                                pageMetric.setPageNumber(pageNum);
                                pageMetrics.put(pageNum, pageMetric);
                            }
                        }
                    } else if (fieldName.equals("POSITIVE_SELECTORS")) {
                        List<String> positiveSelectors = m.getPositiveSelectors();
                        if (positiveSelectors == null) {
                            positiveSelectors = new ArrayList<>();
                        }
                        positiveSelectors.add(fieldValue);
                        m.setPositiveSelectors(positiveSelectors);
                    } else if (fieldName.equals("NEGATIVE_SELECTORS")) {
                        List<String> negativeSelectors = m.getNegativeSelectors();
                        if (negativeSelectors == null) {
                            negativeSelectors = new ArrayList<>();
                        }
                        negativeSelectors.add(fieldValue);
                        m.setNegativeSelectors(negativeSelectors);
                    } else if (fieldName.equals("LAST_UPDATED")) {
                        try {
                            Date d = sdf_date_time2.parse(fieldValue);
                            m.setLastUpdated(d);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else if (fieldName.equals("NUM_UPDATES")) {
                        try {
                            long numUpdates = Long.parseLong(fieldValue);
                            m.setNumUpdates(numUpdates);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else if (fieldName.equals("QUERY_NAME")) {
                        m.setQueryName(fieldValue);
                    } else if (fieldName.equals("PARAMETERS")) {
                        if (fieldValue != null) {
                            try {
                                Set<Parameter> parameters = QueryUtil.parseParameters(fieldValue);
                                m.setParameters(parameters);
                                
                            } catch (Exception e) {
                                log.debug(e.getMessage());
                            }
                        }
                    } else if (fieldName.equals("SOURCE_COUNT")) {
                        m.setSourceCount(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("NEXT_COUNT")) {
                        m.setNextCount(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("SEEK_COUNT")) {
                        m.setSeekCount(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("YIELD_COUNT")) {
                        m.setYieldCount(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("DOC_RANGES")) {
                        m.setDocRanges(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("FI_RANGES")) {
                        m.setFiRanges(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("VERSION")) {
                        m.setVersion(fieldValue);
                    } else if (fieldName.equals("YIELD_COUNT")) {
                        m.setYieldCount(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("LOGIN_TIME")) {
                        m.setLoginTime(Long.parseLong(fieldValue));
                    } else {
                        log.debug("encountered unanticipated field name: " + fieldName);
                    }
                }
            }
            // if createDate has not been set, try to parse it from the event row
            if (!createDateSet) {
                try {
                    String dateStr = event.getMetadata().getRow().substring(0, 8);
                    m.setCreateDate(sdf_date_time3.parse(dateStr));
                } catch (ParseException e) {
                    
                }
            }
            m.setPageTimes(new ArrayList<>(pageMetrics.values()));
            return m;
        } catch (RuntimeException e) {
            return null;
        }
    }
    
    protected void createAndConfigureTablesIfNecessary(String[] tableNames, TableOperations tops, Configuration conf)
                    throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        for (String table : tableNames) {
            // If the tables don't exist, then create them.
            try {
                if (!tops.exists(table)) {
                    tops.create(table);
                    Map<String,TableConfigHelper> tableConfigs = getTableConfigs(log, conf, tableNames);
                    
                    TableConfigHelper tableHelper = tableConfigs.get(table);
                    
                    if (tableHelper != null) {
                        tableHelper.configure(tops);
                    } else {
                        log.info("No configuration supplied for table: " + table);
                    }
                }
            } catch (TableExistsException te) {
                // in this case, somebody else must have created the table after our existence check
                log.debug("Tried to create " + table + " but somebody beat us to the punch");
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private Map<String,TableConfigHelper> getTableConfigs(Logger log, Configuration conf, String[] tableNames) {
        Map<String,TableConfigHelper> helperMap = new HashMap<>();
        
        for (String table : tableNames) {
            String prop = table + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX;
            String className = conf.get(prop, null);
            TableConfigHelper tableHelper = null;
            
            if (className != null) {
                try {
                    Class<? extends TableConfigHelper> tableHelperClass = (Class<? extends TableConfigHelper>) Class.forName(className.trim());
                    tableHelper = tableHelperClass.getDeclaredConstructor().newInstance();
                    
                    if (tableHelper != null)
                        tableHelper.setup(table, conf, log);
                } catch (Exception e) {
                    throw new IllegalArgumentException(e);
                }
            }
            
            helperMap.put(table, tableHelper);
        }
        
        return helperMap;
    }
    
    @Override
    public void reload() {
        try {
            if (this.recordWriter != null) {
                // don't try to flush the mtbw (close). If recordWriter != null then this method is being called
                // because of an Exception and the metrics have been saved off to be added to the new recordWriter.
                this.recordWriter.returnConnector();
            }
            recordWriter = new AccumuloRecordWriter(connectionPool, conf);
        } catch (AccumuloException | AccumuloSecurityException | IOException e) {
            log.error(e.getMessage(), e);
        }
    }
    
    @Override
    public ContentQueryMetricsIngestHelper getQueryMetricsIngestHelper(boolean deleteMode) {
        return new ContentQueryMetricsIngestHelper(deleteMode);
    }
    
    @Override
    public QueryMetricsSummaryResponse getQueryMetricsSummary(Date begin, Date end, ProxiedUserDetails currentUser, boolean onlyCurrentUser) {
        QueryMetricsSummaryResponse response = new QueryMetricsSummaryResponse();
        
        try {
            // this method is open to any user
            DatawaveUser datawaveUser = currentUser.getPrimaryUser();
            String datawaveUserShortName = DnUtils.getShortName(datawaveUser.getName());
            Collection<String> userAuths = new ArrayList<>(datawaveUser.getAuths());
            if (connectorAuthorizations != null) {
                Collection<String> connectorAuths = new ArrayList<>();
                Arrays.stream(StringUtils.split(connectorAuthorizations, ',')).forEach(a -> {
                    connectorAuths.add(a);
                });
                userAuths.retainAll(connectorAuths);
            }
            Collection<? extends Collection<String>> authorizations = Collections.singletonList(userAuths);
            Query query = createQuery();
            query.setBeginDate(begin);
            query.setEndDate(end);
            query.setQueryLogicName(QUERY_METRICS_LOGIC_NAME);
            if (onlyCurrentUser) {
                query.setQuery("USER == '" + datawaveUserShortName + "'");
            } else {
                query.setQuery("((_Bounded_ = true) && (USER > 'A' && USER < 'ZZZZZZZ'))");
            }
            query.setQueryName(QUERY_METRICS_LOGIC_NAME);
            query.setColumnVisibility(queryMetricHandlerProperties.getQueryVisibility());
            query.setQueryAuthorizations(AuthorizationsUtil.buildAuthorizationString(authorizations));
            query.setExpirationDate(DateUtils.addDays(new Date(), 1));
            query.setPagesize(1000);
            query.setUserDN(datawaveUserShortName);
            query.setId(UUID.randomUUID());
            query.setParameters(ImmutableMap.of(QueryOptions.INCLUDE_GROUPING_CONTEXT, "true"));
            
            List<T> queryMetrics = getQueryMetrics(response, query);
            List<QueryExceptionType> exceptions = response.getExceptions();
            if (exceptions == null || exceptions.isEmpty()) {
                response = processQueryMetricsSummary(queryMetrics, end);
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            response.addException(e);
        }
        return response;
    }
}
