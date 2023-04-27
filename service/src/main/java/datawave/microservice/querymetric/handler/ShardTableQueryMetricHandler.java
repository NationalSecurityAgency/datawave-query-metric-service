package datawave.microservice.querymetric.handler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import datawave.ingest.table.config.TableConfigHelper;
import datawave.marking.MarkingFunctions;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Lifecycle;
import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.microservice.querymetric.QueryMetricsSummaryResponse;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.query.iterator.QueryOptions;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.AuthorizationsUtil;
import datawave.security.util.DnUtils;
import datawave.webservice.common.connection.AccumuloClientPool;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.query.util.QueryUtil;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;

import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static datawave.security.authorization.DatawaveUser.UserType.USER;

public class ShardTableQueryMetricHandler<T extends BaseQueryMetric> extends BaseQueryMetricHandler<T> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final org.apache.log4j.Logger setupLogger = org.apache.log4j.Logger.getLogger(getClass());
    
    protected static final String QUERY_METRICS_LOGIC_NAME = "QueryMetricsQuery";
    protected String clientAuthorizations;
    
    protected AccumuloClientPool accumuloClientPool;
    protected QueryMetricHandlerProperties queryMetricHandlerProperties;
    
    @SuppressWarnings("FieldCanBeLocal")
    protected final String JOB_ID = "job_201109071404_1";
    
    protected final Configuration conf = new Configuration();
    protected final StatusReporter reporter = new MockStatusReporter();
    protected final AtomicBoolean tablesChecked = new AtomicBoolean(false);
    protected AccumuloRecordWriter recordWriter = null;
    protected QueryMetricQueryLogicFactory logicFactory;
    protected QueryMetricFactory metricFactory;
    protected UIDBuilder<UID> uidBuilder = UID.builder();
    protected DatawavePrincipal datawavePrincipal;
    protected MarkingFunctions markingFunctions;
    protected QueryMetricCombiner queryMetricCombiner;
    protected ExecutorService executorService;
    // this lock is necessary for when there is an error condition and the accumuloRecordWriter needs to be replaced
    protected ReentrantReadWriteLock accumuloRecordWriterLock = new ReentrantReadWriteLock();
    
    public ShardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties,
                    @Qualifier("warehouse") AccumuloClientPool accumuloClientPool, QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory,
                    MarkingFunctions markingFunctions, QueryMetricCombiner queryMetricCombiner, LuceneToJexlQueryParser luceneToJexlQueryParser) {
        super(luceneToJexlQueryParser);
        this.queryMetricHandlerProperties = queryMetricHandlerProperties;
        this.logicFactory = logicFactory;
        this.metricFactory = metricFactory;
        this.markingFunctions = markingFunctions;
        this.accumuloClientPool = accumuloClientPool;
        this.queryMetricCombiner = queryMetricCombiner;
        this.executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("metric-handler-query-thread-%d").build());
        
        queryMetricHandlerProperties.getProperties().entrySet().forEach(e -> conf.set(e.getKey(), e.getValue()));
        
        AccumuloClient accumuloClient = null;
        try {
            log.info("creating connector with username:" + queryMetricHandlerProperties.getUsername());
            Map<String,String> trackingMap = AccumuloClientTracking.getTrackingMap(Thread.currentThread().getStackTrace());
            accumuloClient = accumuloClientPool.borrowObject(trackingMap);
            this.clientAuthorizations = accumuloClient.securityOperations().getUserAuthorizations(accumuloClient.whoami()).toString();
            reload();
            
            if (this.tablesChecked.compareAndSet(false, true)) {
                verifyTables();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            if (accumuloClient != null) {
                this.accumuloClientPool.returnObject(accumuloClient);
            }
        }
        Collection<String> auths = new ArrayList<>();
        if (this.clientAuthorizations != null) {
            Arrays.stream(StringUtils.split(this.clientAuthorizations, ',')).forEach(a -> {
                auths.add(a);
            });
        }
        DatawaveUser datawaveUser = new DatawaveUser(SubjectIssuerDNPair.of("admin"), USER, null, auths, null, null, System.currentTimeMillis());
        this.datawavePrincipal = new DatawavePrincipal(Collections.singletonList(datawaveUser));
    }
    
    public void shutdown() throws Exception {
        if (this.recordWriter != null) {
            this.accumuloRecordWriterLock.writeLock().lock();
            try {
                this.recordWriter.close(null);
                this.recordWriter = null;
            } finally {
                this.accumuloRecordWriterLock.writeLock().unlock();
            }
        }
    }
    
    @Override
    public void flush() throws Exception {
        if (this.recordWriter != null) {
            this.accumuloRecordWriterLock.readLock().lock();
            try {
                this.recordWriter.flush();
            } finally {
                this.accumuloRecordWriterLock.readLock().unlock();
            }
        }
    }
    
    public void verifyTables() {
        AccumuloClient accumuloClient = null;
        try {
            AbstractColumnBasedHandler<Key> handler = new ContentIndexingColumnBasedHandler() {
                @Override
                public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
                    return getQueryMetricsIngestHelper(false);
                }
            };
            Map<String,String> trackingMap = AccumuloClientTracking.getTrackingMap(Thread.currentThread().getStackTrace());
            accumuloClient = accumuloClientPool.borrowObject(trackingMap);
            createAndConfigureTablesIfNecessary(handler.getTableNames(conf), accumuloClient, conf);
        } catch (Exception e) {
            log.error("Error verifying table configuration", e);
        } finally {
            if (accumuloClient != null) {
                this.accumuloClientPool.returnObject(accumuloClient);
            }
        }
    }
    
    public void writeMetric(T updatedQueryMetric, List<T> storedQueryMetrics, Date lastUpdated, boolean delete) throws Exception {
        try {
            TaskAttemptID taskId = new TaskAttemptID(new TaskID(new JobID(JOB_ID, 1), TaskType.MAP, 1), 1);
            
            this.accumuloRecordWriterLock.readLock().lock();
            try {
                MapContext<Text,RawRecordContainer,Text,Mutation> context = new MapContextImpl<>(conf, taskId, null, this.recordWriter, null, reporter, null);
                for (T storedQueryMetric : storedQueryMetrics) {
                    ContentIndexingColumnBasedHandler handler = new ContentIndexingColumnBasedHandler() {
                        @Override
                        public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
                            return getQueryMetricsIngestHelper(delete);
                        }
                    };
                    handler.setup(context);
                    Multimap<BulkIngestKey,Value> r = getEntries(handler, updatedQueryMetric, storedQueryMetric, lastUpdated);
                    if (r != null) {
                        for (Entry<BulkIngestKey,Value> e : r.entries()) {
                            recordWriter.write(e.getKey().getTableName(), getMutation(e.getKey().getKey(), e.getValue()));
                        }
                    }
                    if (!delete && handler.getMetadata() != null) {
                        for (Entry<BulkIngestKey,Value> e : handler.getMetadata().getBulkMetadata().entries()) {
                            recordWriter.write(e.getKey().getTableName(), getMutation(e.getKey().getKey(), e.getValue()));
                        }
                    }
                }
            } finally {
                this.accumuloRecordWriterLock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            // assume that an error happened with the AccumuloRecordWriter
            // mark recordWriter as unhealthy -- the first thread to get the writeLock in
            // reload will create a new one that will be marked healthy
            this.recordWriter.setHealthy(false);
            reload();
            // we have no way of knowing if the rejected mutation is this one or a previously
            // written one throw the exception so that the metric will be re-written
            throw e;
        }
    }
    
    private Mutation getMutation(Key key, Value value) {
        Mutation m = new Mutation(key.getRow());
        if (key.isDeleted()) {
            m.putDelete(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp());
        } else {
            m.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value);
        }
        return m;
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
        boolean deleteMode = ingestHelper.getDeleteMode();
        ingestHelper.setup(conf);
        
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
            
            String value = currentKey.getRow().toString();
            if (value.length() > fieldSizeThreshold && (table.equals(indexTable) || table.equals(reverseIndexTable))) {
                keysToRemove.add(bulkIngestKey);
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
    public T combineMetrics(T updatedQueryMetric, T cachedQueryMetric, QueryMetricType metricType) throws Exception {
        return (T) queryMetricCombiner.combineMetrics(updatedQueryMetric, cachedQueryMetric, metricType);
    }
    
    public T getQueryMetric(final String queryId) throws Exception {
        List<T> queryMetrics = getQueryMetrics("QUERY_ID == '" + queryId + "'");
        return queryMetrics.isEmpty() ? null : queryMetrics.get(0);
    }
    
    public Query createQuery() {
        return new QueryImpl();
    }
    
    public List<T> getQueryMetrics(final String query) throws Exception {
        Date end = new Date();
        Date begin = DateUtils.setYears(end, 2000);
        Query queryImpl = createQuery();
        queryImpl.setBeginDate(begin);
        queryImpl.setEndDate(end);
        queryImpl.setQueryLogicName(QUERY_METRICS_LOGIC_NAME);
        queryImpl.setQuery(query);
        queryImpl.setQueryName(QUERY_METRICS_LOGIC_NAME);
        queryImpl.setColumnVisibility(queryMetricHandlerProperties.getQueryVisibility());
        queryImpl.setQueryAuthorizations(this.clientAuthorizations);
        queryImpl.setExpirationDate(DateUtils.addDays(new Date(), 1));
        queryImpl.setPagesize(1000);
        queryImpl.setId(UUID.randomUUID());
        queryImpl.setParameters(ImmutableMap.of(QueryOptions.INCLUDE_GROUPING_CONTEXT, "true"));
        return getQueryMetrics(queryImpl);
    }
    
    public List<T> getQueryMetrics(Query query) throws Exception {
        long startTime = System.currentTimeMillis();
        long maxReadMilliseconds = queryMetricHandlerProperties.getMaxReadMilliseconds();
        List<T> queryMetrics = new ArrayList<>();
        AccumuloClient accumuloClient = null;
        try {
            QueryLogic<?> queryLogic = logicFactory.getObject();
            Map<String,String> trackingMap = AccumuloClientTracking.getTrackingMap(Thread.currentThread().getStackTrace());
            accumuloClient = accumuloClientPool.borrowObject(trackingMap);
            final AccumuloClient finalClient = accumuloClient;
            // create RunningQuery inside an Executor/Future so that we can handle a non-responsive Accumulo and timeout
            Future<Object> future1 = this.executorService.submit(() -> {
                try {
                    return new RunningQuery(null, finalClient, Priority.ADMIN, queryLogic, query, query.getQueryAuthorizations(), datawavePrincipal,
                                    this.metricFactory);
                } catch (Exception e) {
                    return e;
                }
            });
            
            RunningQuery runningQuery;
            try {
                Object o = future1.get(maxReadMilliseconds - (System.currentTimeMillis() - startTime), TimeUnit.MILLISECONDS);
                if (o instanceof RunningQuery) {
                    runningQuery = (RunningQuery) o;
                } else {
                    throw (Exception) o;
                }
            } finally {
                future1.cancel(true);
            }
            
            if (runningQuery != null) {
                // Call RunningQuery.next inside an Executor/Future so that we can handle a non-responsive Accumulo and timeout
                Future<Object> future2 = this.executorService.submit(() -> {
                    try {
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
                            throw new RuntimeException(exceptions.get(0).getMessage());
                        }
                        
                        if (!(queryResponse instanceof EventQueryResponseBase)) {
                            throw new IllegalStateException("incompatible response");
                        }
                        
                        EventQueryResponseBase eventQueryResponse = (EventQueryResponseBase) queryResponse;
                        List<EventBase> eventList = eventQueryResponse.getEvents();

                        if (eventList != null) {
                            for (EventBase<?, ?> event : eventList) {
                                T metric = toMetric(event);
                                queryMetrics.add(metric);
                            }
                        }
                        return null;
                    } catch (Exception e) {
                        return e;
                    }
                });
                try {
                    Exception exception = (Exception) future2.get(maxReadMilliseconds - (System.currentTimeMillis() - startTime), TimeUnit.MILLISECONDS);
                    if (exception != null) {
                        throw exception;
                    }
                } finally {
                    future2.cancel(true);
                }
            }
        } finally {
            if (accumuloClient != null) {
                this.accumuloClientPool.returnObject(accumuloClient);
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
            T m = (T) metricFactory.createMetric(false);
            List<FieldBase> field = event.getFields();
            m.setMarkings(event.getMarkings());
            TreeMap<Long,PageMetric> pageMetrics = Maps.newTreeMap();
            
            boolean createDateSet = false;
            for (FieldBase f : field) {
                String fieldName = f.getName();
                String fieldValue = f.getValueString();
                if (!excludedFields.contains(fieldName)) {
                    
                    if (fieldName.equals("AUTHORIZATIONS")) {
                        m.setQueryAuthorizations(fieldValue);
                    } else if (fieldName.equals("BEGIN_DATE")) {
                        try {
                            Date d = sdf_date_time1.parse(fieldValue);
                            m.setBeginDate(d);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else if (fieldName.equals("CREATE_CALL_TIME")) {
                        m.setCreateCallTime(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("CREATE_DATE")) {
                        try {
                            Date d = sdf_date_time2.parse(fieldValue);
                            m.setCreateDate(d);
                            createDateSet = true;
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else if (fieldName.equals("DOC_RANGES")) {
                        m.setDocRanges(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("END_DATE")) {
                        try {
                            Date d = sdf_date_time1.parse(fieldValue);
                            m.setEndDate(d);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else if (fieldName.equals("ERROR_CODE")) {
                        m.setErrorCode(fieldValue);
                    } else if (fieldName.equals("ERROR_MESSAGE")) {
                        m.setErrorMessage(fieldValue);
                    } else if (fieldName.equals("FI_RANGES")) {
                        m.setFiRanges(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("HOST")) {
                        m.setHost(fieldValue);
                    } else if (fieldName.equals("LAST_UPDATED")) {
                        try {
                            Date d = sdf_date_time2.parse(fieldValue);
                            m.setLastUpdated(d);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else if (fieldName.equals("LIFECYCLE")) {
                        m.setLifecycle(Lifecycle.valueOf(fieldValue));
                    } else if (fieldName.equals("LOGIN_TIME")) {
                        m.setLoginTime(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("NEGATIVE_SELECTORS")) {
                        List<String> negativeSelectors = m.getNegativeSelectors();
                        if (negativeSelectors == null) {
                            negativeSelectors = new ArrayList<>();
                        }
                        negativeSelectors.add(fieldValue);
                        m.setNegativeSelectors(negativeSelectors);
                    } else if (fieldName.equals("NEXT_COUNT")) {
                        m.setNextCount(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("NUM_UPDATES")) {
                        try {
                            long numUpdates = Long.parseLong(fieldValue);
                            m.setNumUpdates(numUpdates);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
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
                    } else if (fieldName.equals("PARAMETERS")) {
                        if (fieldValue != null) {
                            try {
                                m.setParameters(QueryUtil.parseParameters(fieldValue));
                            } catch (Exception e) {
                                log.error(e.getMessage());
                            }
                        }
                    } else if (fieldName.equals("PLAN")) {
                        m.setPlan(fieldValue);
                    } else if (fieldName.equals("POSITIVE_SELECTORS")) {
                        List<String> positiveSelectors = m.getPositiveSelectors();
                        if (positiveSelectors == null) {
                            positiveSelectors = new ArrayList<>();
                        }
                        positiveSelectors.add(fieldValue);
                        m.setPositiveSelectors(positiveSelectors);
                    } else if (fieldName.equals("PREDICTION")) {
                        if (fieldValue != null) {
                            try {
                                int x = fieldValue.indexOf(":");
                                if (x > -1) {
                                    String predictionName = fieldValue.substring(0, x);
                                    Double predictionValue = Double.parseDouble(fieldValue.substring(x + 1));
                                    m.addPrediction(new Prediction(predictionName, predictionValue));
                                }
                            } catch (Exception e) {
                                log.error(e.getMessage());
                            }
                        }
                    } else if (fieldName.equals("PROXY_SERVERS")) {
                        m.setProxyServers(Arrays.asList(StringUtils.split(fieldValue, ",")));
                    } else if (fieldName.equals("QUERY")) {
                        m.setQuery(fieldValue);
                    } else if (fieldName.equals("QUERY_ID")) {
                        m.setQueryId(fieldValue);
                    } else if (fieldName.equals("QUERY_LOGIC")) {
                        m.setQueryLogic(fieldValue);
                    } else if (fieldName.equals("QUERY_NAME")) {
                        m.setQueryName(fieldValue);
                    } else if (fieldName.equals("QUERY_TYPE")) {
                        m.setQueryType(fieldValue);
                    } else if (fieldName.equals("SEEK_COUNT")) {
                        m.setSeekCount(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("SETUP_TIME")) {
                        m.setSetupTime(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("SOURCE_COUNT")) {
                        m.setSourceCount(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("USER")) {
                        m.setUser(fieldValue);
                    } else if (fieldName.equals("USER_DN")) {
                        m.setUserDN(fieldValue);
                    } else if (fieldName.equals("VERSION")) {
                        m.addVersion(BaseQueryMetric.DATAWAVE, fieldValue);
                    } else if (fieldName.startsWith("VERSION.")) {
                        m.addVersion(fieldName.substring(8), fieldValue);
                    } else if (fieldName.equals("YIELD_COUNT")) {
                        m.setYieldCount(Long.parseLong(fieldValue));
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
    
    protected void createAndConfigureTablesIfNecessary(String[] tableNames, AccumuloClient accumuloClient, Configuration conf)
                    throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        for (String table : tableNames) {
            // If the tables don't exist, then create them.
            try {
                String[] tableNameSplit = StringUtils.split(table, '.');
                if (tableNameSplit.length > 1) {
                    String namespace = tableNameSplit[0];
                    if (!accumuloClient.namespaceOperations().exists(namespace)) {
                        try {
                            accumuloClient.namespaceOperations().create(namespace);
                        } catch (NamespaceExistsException e) {
                            log.error(e.getMessage());
                        }
                    }
                }
                if (!accumuloClient.tableOperations().exists(table)) {
                    accumuloClient.tableOperations().create(table);
                    Map<String,TableConfigHelper> tableConfigs = getTableConfigs(conf, tableNames);
                    
                    TableConfigHelper tableHelper = tableConfigs.get(table);
                    
                    if (tableHelper != null) {
                        tableHelper.configure(accumuloClient.tableOperations());
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
    private Map<String,TableConfigHelper> getTableConfigs(Configuration conf, String[] tableNames) {
        Map<String,TableConfigHelper> helperMap = new HashMap<>();
        
        for (String table : tableNames) {
            String prop = table + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX;
            String className = conf.get(prop, null);
            TableConfigHelper tableHelper = null;
            
            if (className != null) {
                try {
                    Class<? extends TableConfigHelper> tableHelperClass = (Class<? extends TableConfigHelper>) Class.forName(className.trim());
                    tableHelper = tableHelperClass.getDeclaredConstructor().newInstance();
                    
                    if (tableHelper != null) {
                        tableHelper.setup(table, conf, setupLogger);
                    }
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
        this.accumuloRecordWriterLock.writeLock().lock();
        try {
            if (this.recordWriter == null || !this.recordWriter.isHealthy()) {
                log.info("creating new AccumuloRecordWriter");
                try {
                    if (this.recordWriter != null) {
                        // don't try to flush the mtbw (close). If recordWriter != null then this method is being called
                        // because of an Exception and the writing of the metrics should be re-tried with the new recordWriter.
                        this.recordWriter.returnConnector();
                    }
                    this.recordWriter = new AccumuloRecordWriter(accumuloClientPool, conf);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        } finally {
            this.accumuloRecordWriterLock.writeLock().unlock();
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
            if (clientAuthorizations != null) {
                Collection<String> connectorAuths = new ArrayList<>();
                Arrays.stream(StringUtils.split(clientAuthorizations, ',')).forEach(a -> {
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
            
            List<T> queryMetrics = getQueryMetrics(query);
            response = processQueryMetricsSummary(queryMetrics, end);
            
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(e);
        }
        return response;
    }
}
