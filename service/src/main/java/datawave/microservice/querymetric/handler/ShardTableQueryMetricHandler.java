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
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.query.iterator.QueryOptions;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.common.connection.AccumuloConnectionPool;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.query.logic.QueryLogic;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Lifecycle;
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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;
import java.nio.charset.Charset;
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

public class ShardTableQueryMetricHandler<T extends BaseQueryMetric> implements QueryMetricHandler<T> {
    private static final Logger log = ThreadConfigurableLogger.getLogger(ShardTableQueryMetricHandler.class.getName());
    
    private static final String QUERY_METRICS_LOGIC_NAME = "QueryMetricsQuery";
    
    private AccumuloConnectionPool connectionPool;
    private QueryMetricHandlerProperties queryMetricHandlerProperties;
    
    private String connectorAuthorizations = null;
    
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
            enableLogs(false);
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
            enableLogs(false);
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
        if (updatedQueryMetric.getMarkings() != null) {
            try {
                event.setVisibility(this.markingFunctions.translateToColumnVisibility(updatedQueryMetric.getMarkings()));
            } catch (MarkingFunctions.Exception e) {
                log.error(e.getMessage(), e);
                event.setSecurityMarkings(this.queryMetricHandlerProperties.getDefaultMetricMarkings());
            }
        } else {
            event.setSecurityMarkings(this.queryMetricHandlerProperties.getDefaultMetricMarkings());
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
    
    @SuppressWarnings("unchecked")
    @Override
    public T combineMetrics(T updatedQueryMetric, T cachedQueryMetric) throws Exception {
        
        try {
            enableLogs(false);
            if (cachedQueryMetric != null) {
                Map<Long,PageMetric> storedPageMetricMap = new TreeMap<>();
                if (cachedQueryMetric.getPageTimes() != null) {
                    cachedQueryMetric.getPageTimes().forEach(pm -> storedPageMetricMap.put(pm.getPageNumber(), pm));
                }
                // combine all of the page metrics from the cached metric and the updated metric
                if (updatedQueryMetric.getPageTimes() != null) {
                    updatedQueryMetric.getPageTimes().forEach(pm -> storedPageMetricMap.put(pm.getPageNumber(), pm));
                }
                ArrayList<PageMetric> newPageMetrics = new ArrayList<>(storedPageMetricMap.values());
                updatedQueryMetric.setNumUpdates(cachedQueryMetric.getNumUpdates() + 1);
                updatedQueryMetric.setPageTimes(newPageMetrics);
                
                // use updated lifecycle unless trying to update a final lifecycle with a non-final lifecycle
                if (cachedQueryMetric.isLifecycleFinal() && !updatedQueryMetric.isLifecycleFinal()) {
                    updatedQueryMetric.setLifecycle(cachedQueryMetric.getLifecycle());
                }
            }
            return updatedQueryMetric;
        } finally {
            enableLogs(true);
        }
    }
    
    public T getQueryMetric(final String queryId) {
        List<T> queryMetrics;
        try {
            enableLogs(false);
            VoidResponse response = new VoidResponse();
            queryMetrics = getQueryMetrics(response, "QUERY_ID == '" + queryId + "'");
            List<QueryExceptionType> exceptions = response.getExceptions();
            if (exceptions != null && !exceptions.isEmpty()) {
                exceptions.forEach(e -> {
                    log.error(e.getMessage());
                });
            }
        } finally {
            enableLogs(true);
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
        
        List<String> excludedFields = Arrays.asList("ELAPSED_TIME", "RECORD_ID", "NUM_PAGES", "NUM_RESULTS");
        
        try {
            T m = (T) metricFactory.createMetric();
            List<FieldBase> field = event.getFields();
            m.setMarkings(event.getMarkings());
            TreeMap<Long,PageMetric> pageMetrics = Maps.newTreeMap();
            
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
                            Long repetition = Long.parseLong(fieldName.substring(index + 1));
                            
                            String[] parts = StringUtils.split(fieldValue, "/");
                            PageMetric pageMetric = null;
                            if (parts.length == 8) {
                                pageMetric = new PageMetric(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]),
                                                Long.parseLong(parts[3]), Long.parseLong(parts[4]), Long.parseLong(parts[5]), Long.parseLong(parts[6]),
                                                Long.parseLong(parts[7]));
                            } else if (parts.length == 7) {
                                pageMetric = new PageMetric(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]),
                                                Long.parseLong(parts[3]), Long.parseLong(parts[4]), Long.parseLong(parts[5]), Long.parseLong(parts[6]));
                            } else if (parts.length == 5) {
                                pageMetric = new PageMetric(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]),
                                                Long.parseLong(parts[3]), Long.parseLong(parts[4]), 0l, 0l);
                            } else if (parts.length == 2) {
                                pageMetric = new PageMetric(Long.parseLong(parts[0]), Long.parseLong(parts[1]), 0l, 0l);
                            }
                            
                            if (pageMetric != null)
                                pageMetrics.put(repetition, pageMetric);
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
                    } else if (fieldName.equals("YIELD_COUNT")) {
                        m.setYieldCount(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("LOGIN_TIME")) {
                        m.setLoginTime(Long.parseLong(fieldValue));
                    } else {
                        log.debug("encountered unanticipated field name: " + fieldName);
                    }
                }
            }
            
            for (final Entry<Long,PageMetric> entry : pageMetrics.entrySet())
                m.addPageMetric(entry.getValue());
            
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
    
    protected void enableLogs(boolean enable) {
        if (enable) {
            ThreadConfigurableLogger.clearThreadLevels();
        } else {
            // All loggers that are encountered in the call chain during metrics calls should be included here.
            // If you need to add a logger name here, you also need to change the Logger declaration where that Logger is instantiated
            // Change:
            // Logger log = Logger.getLogger(MyClass.class);
            // to
            // Logger log = ThreadConfigurableLogger.getLogger(MyClass.class);
            
            ThreadConfigurableLogger.setLevelForThread("datawave.query.index.lookup.RangeStream", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.metrics.ShardTableQueryMetricHandler", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.planner.DefaultQueryPlanner", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.planner.ThreadedRangeBundlerIterator", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.scheduler.SequentialScheduler", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.tables.ShardQueryLogic", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.metrics.ShardTableQueryMetricHandler", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.jexl.visitors.QueryModelVisitor", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.jexl.visitors.ExpandMultiNormalizedTerms", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.jexl.lookups.LookupBoundedRangeForTerms", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.jexl.visitors.RangeConjunctionRebuildingVisitor", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.config.ShardQueryConfiguration", Level.ERROR);
            
            ThreadConfigurableLogger.setLevelForThread("datawave.ingest.data.TypeRegistry", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.ingest.data.config.ingest.BaseIngestHelper", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.ingest.util.RegionTimer", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.ingest.data.Event", Level.TRACE);
            ThreadConfigurableLogger.setLevelForThread("datawave.microservice.querymetric.handler.AccumuloRecordWriter", Level.ERROR);
        }
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
}
