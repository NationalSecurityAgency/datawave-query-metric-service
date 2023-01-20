package datawave.microservice.querymetric;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.marking.MarkingFunctions;
import datawave.microservice.authorization.preauth.ProxiedEntityX509Filter;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.querymetric.config.QueryMetricClientProperties;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.function.QueryMetricSupplier;
import datawave.microservice.querymetric.handler.QueryMetricCombiner;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.security.util.DnUtils;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.messaging.Message;
import org.springframework.web.client.RestTemplate;

import javax.inject.Named;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static datawave.microservice.querymetric.config.HazelcastMetricCacheConfiguration.INCOMING_METRICS;
import static datawave.microservice.querymetric.config.HazelcastMetricCacheConfiguration.LAST_WRITTEN_METRICS;
import static datawave.security.authorization.DatawaveUser.UserType.USER;

public class QueryMetricTestBase {
    
    protected Logger log = LoggerFactory.getLogger(getClass());
    
    protected static final SubjectIssuerDNPair ALLOWED_CALLER = SubjectIssuerDNPair.of("cn=test a. user, ou=example developers, o=example corp, c=us",
                    "cn=example corp ca, o=example corp, c=us");
    
    protected static final String getMetricsUrl = "/querymetric/v1/id/%s";
    
    @Autowired
    protected RestTemplateBuilder restTemplateBuilder;
    
    @Autowired
    protected JWTTokenHandler jwtTokenHandler;
    
    @Autowired
    protected ObjectMapper objectMapper;
    
    @Autowired
    protected ShardTableQueryMetricHandler shardTableQueryMetricHandler;
    
    @Autowired
    protected QueryMetricCombiner queryMetricCombiner;
    
    @Autowired
    @Named("queryMetricCacheManager")
    protected CacheManager cacheManager;
    
    @Autowired
    protected @Qualifier("warehouse") Connector connector;
    
    @Autowired
    protected QueryMetricHandlerProperties queryMetricHandlerProperties;
    
    @Autowired
    protected QueryMetricFactory queryMetricFactory;
    
    @Autowired
    protected QueryMetricClient client;
    
    @Autowired
    private QueryMetricClientProperties queryMetricClientProperties;
    
    @Autowired
    private DnUtils dnUtils;
    
    protected Cache incomingQueryMetricsCache;
    protected Cache lastWrittenQueryMetricCache;
    
    @LocalServerPort
    protected int webServicePort;
    
    protected RestTemplate restTemplate;
    protected DatawaveUserDetails adminUser;
    protected DatawaveUserDetails nonAdminUser;
    protected static boolean isHazelCast;
    protected static CacheManager staticCacheManager;
    protected static Map<String,String> metricMarkings;
    protected List<String> tables;
    protected Collection<String> auths;
    
    static {
        metricMarkings = new HashMap<>();
        metricMarkings.put(MarkingFunctions.Default.COLUMN_VISIBILITY, "A&C");
    }
    
    @AfterAll
    public static void afterClass() {
        ((HazelcastCacheManager) staticCacheManager).getHazelcastInstance().shutdown();
    }
    
    @BeforeEach
    public void setup() {
        this.queryMetricClientProperties.setPort(webServicePort);
        this.restTemplate = restTemplateBuilder.build(RestTemplate.class);
        this.auths = Arrays.asList("PUBLIC", "A", "B", "C");
        
        Collection<String> roles = Arrays.asList("Administrator");
        DatawaveUser adminDWUser = new DatawaveUser(ALLOWED_CALLER, USER, null, auths, roles, null, System.currentTimeMillis());
        DatawaveUser nonAdminDWUser = new DatawaveUser(ALLOWED_CALLER, USER, null, auths, null, null, System.currentTimeMillis());
        this.adminUser = new DatawaveUserDetails(Collections.singleton(adminDWUser), adminDWUser.getCreationTime());
        this.nonAdminUser = new DatawaveUserDetails(Collections.singleton(nonAdminDWUser), nonAdminDWUser.getCreationTime());
        QueryMetricTestBase.isHazelCast = cacheManager instanceof HazelcastCacheManager;
        QueryMetricTestBase.staticCacheManager = cacheManager;
        this.incomingQueryMetricsCache = cacheManager.getCache(INCOMING_METRICS);
        this.lastWrittenQueryMetricCache = cacheManager.getCache(LAST_WRITTEN_METRICS);
        this.shardTableQueryMetricHandler.verifyTables();
        BaseQueryMetric m = createMetric();
        // this is to ensure that the QueryMetrics_m table
        // is populated so that queries work properly
        try {
            this.shardTableQueryMetricHandler.writeMetric(m, Collections.singletonList(m), m.getLastUpdated(), false);
            this.shardTableQueryMetricHandler.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
        tables = new ArrayList<>();
        tables.add(queryMetricHandlerProperties.getIndexTableName());
        tables.add(queryMetricHandlerProperties.getReverseIndexTableName());
        tables.add(queryMetricHandlerProperties.getShardTableName());
        deleteAccumuloEntries(connector, tables, this.auths);
        Assertions.assertTrue(getMetadataEntries().size() > 0, "metadata table empty");
        SimpleModule baseQueryMetricDeserializer = new SimpleModule(BaseQueryMetricListResponse.class.getName());
        baseQueryMetricDeserializer.addAbstractTypeMapping(BaseQueryMetricListResponse.class, QueryMetricListResponse.class);
        objectMapper.registerModule(baseQueryMetricDeserializer);
    }
    
    @AfterEach
    public void cleanup() {
        deleteAccumuloEntries(connector, tables, this.auths);
        this.incomingQueryMetricsCache.clear();
        this.lastWrittenQueryMetricCache.clear();
    }
    
    protected EventBase toEvent(BaseQueryMetric metric) {
        SimpleDateFormat sdf_date_time1 = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat sdf_date_time2 = new SimpleDateFormat("yyyyMMdd HHmmss");
        
        long createTime = metric.getCreateDate().getTime();
        
        DefaultEvent event = new DefaultEvent();
        List<DefaultField> fields = new ArrayList<>();
        
        event.setMarkings(metric.getMarkings());
        
        addStringField(fields, "QUERY_ID", metric.getColumnVisibility(), createTime, metric.getQueryId());
        addDateField(fields, "BEGIN_DATE", metric.getColumnVisibility(), createTime, metric.getBeginDate(), sdf_date_time1);
        addDateField(fields, "END_DATE", metric.getColumnVisibility(), createTime, metric.getEndDate(), sdf_date_time1);
        addDateField(fields, "LAST_UPDATED", metric.getColumnVisibility(), createTime, metric.getLastUpdated(), sdf_date_time2);
        addLongField(fields, "NUM_UPDATES", metric.getColumnVisibility(), createTime, metric.getNumUpdates());
        addStringField(fields, "QUERY", metric.getColumnVisibility(), createTime, metric.getQuery());
        addStringField(fields, "QUERY_LOGIC", metric.getColumnVisibility(), createTime, metric.getQueryLogic());
        addStringField(fields, "HOST", metric.getColumnVisibility(), createTime, metric.getHost());
        addStringField(fields, "QUERY_TYPE", metric.getColumnVisibility(), createTime, metric.getQueryType());
        addLifecycleField(fields, "LIFECYCLE", metric.getColumnVisibility(), createTime, metric.getLifecycle());
        addLongField(fields, "LOGIN_TIME", metric.getColumnVisibility(), createTime, metric.getLoginTime());
        addDateField(fields, "CREATE_DATE", metric.getColumnVisibility(), createTime, metric.getCreateDate(), sdf_date_time2);
        addLongField(fields, "CREATE_CALL_TIME", metric.getColumnVisibility(), createTime, metric.getCreateCallTime());
        addStringField(fields, "AUTHORIZATIONS", metric.getColumnVisibility(), createTime, metric.getQueryAuthorizations());
        addStringField(fields, "QUERY_NAME", metric.getColumnVisibility(), createTime, metric.getQueryName());
        addLongField(fields, "DOC_RANGES", metric.getColumnVisibility(), createTime, metric.getDocRanges());
        addLongField(fields, "FI_RANGES", metric.getColumnVisibility(), createTime, metric.getFiRanges());
        addStringField(fields, "ERROR_CODE", metric.getColumnVisibility(), createTime, metric.getErrorCode());
        addStringField(fields, "ERROR_MESSAGE", metric.getColumnVisibility(), createTime, metric.getErrorMessage());
        addLongField(fields, "SEEK_COUNT", metric.getColumnVisibility(), createTime, metric.getSeekCount());
        addLongField(fields, "NEXT_COUNT", metric.getColumnVisibility(), createTime, metric.getNextCount());
        addStringField(fields, "USER", metric.getColumnVisibility(), createTime, metric.getUser());
        addStringField(fields, "USER_DN", metric.getColumnVisibility(), createTime, metric.getUserDN());
        addPredictionField(fields, metric.getColumnVisibility(), createTime, metric.getPredictions());
        addStringField(fields, "PLAN", metric.getColumnVisibility(), createTime, metric.getPlan());
        addPageMetricsField(fields, metric.getColumnVisibility(), createTime, metric.getPageTimes());
        
        event.setFields(fields);
        
        return event;
    }
    
    protected void addPageMetricsField(List<DefaultField> fields, String columnVisibility, long timestamp, List<BaseQueryMetric.PageMetric> pageMetrics) {
        if (pageMetrics != null) {
            int page = 1;
            for (BaseQueryMetric.PageMetric pageMetric : pageMetrics) {
                addStringField(fields, "PAGE_METRICS." + page++, columnVisibility, timestamp, pageMetric.toEventString());
            }
        }
    }
    
    protected void addStringField(List<DefaultField> fields, String field, String columnVisibility, long timestamp, String value) {
        if (value != null) {
            fields.add(new DefaultField(field, columnVisibility, timestamp, value));
        }
    }
    
    protected void addLifecycleField(List<DefaultField> fields, String field, String columnVisibility, long timestamp, BaseQueryMetric.Lifecycle value) {
        if (value != null) {
            fields.add(new DefaultField(field, columnVisibility, timestamp, value.name()));
        }
    }
    
    protected void addLongField(List<DefaultField> fields, String field, String columnVisibility, long timestamp, Long value) {
        if (value != null) {
            fields.add(new DefaultField(field, columnVisibility, timestamp, Long.toString(value)));
        }
    }
    
    protected void addDateField(List<DefaultField> fields, String field, String columnVisibility, long timestamp, Date value, SimpleDateFormat sdf) {
        if (value != null) {
            fields.add(new DefaultField(field, columnVisibility, timestamp, sdf.format(value)));
        }
    }
    
    protected void addPredictionField(List<DefaultField> fields, String columnVisibility, long timestamp, Set<BaseQueryMetric.Prediction> value) {
        if (value != null) {
            for (BaseQueryMetric.Prediction prediction : value) {
                if (prediction != null) {
                    addStringField(fields, "PREDICTION", columnVisibility, timestamp,
                                    String.join(":", prediction.getName(), Double.toString(prediction.getPrediction())));
                }
            }
        }
    }
    
    protected BaseQueryMetric createMetric() {
        return createMetric(createQueryId());
    }
    
    protected static BaseQueryMetric createMetric(QueryMetricFactory queryMetricFactory) {
        return createMetric(createQueryId(), queryMetricFactory);
    }
    
    protected BaseQueryMetric createMetric(String queryId) {
        return createMetric(queryId, this.queryMetricFactory);
    }
    
    protected static BaseQueryMetric createMetric(String queryId, QueryMetricFactory queryMetricFactory) {
        BaseQueryMetric m = queryMetricFactory.createMetric();
        populateMetric(m, queryId);
        return m;
    }
    
    protected static void populateMetric(BaseQueryMetric m, String queryId) {
        long now = System.currentTimeMillis();
        Date nowDate = new Date(now);
        m.setQueryId(queryId);
        m.setMarkings(metricMarkings);
        m.setEndDate(nowDate);
        m.setBeginDate(DateUtils.addDays(nowDate, -1));
        m.setLastUpdated(nowDate);
        m.setQuery("USER:testuser");
        m.setQueryLogic("QueryMetricsQuery");
        m.setHost("localhost");
        m.setQueryType("RunningQuery");
        m.setLifecycle(BaseQueryMetric.Lifecycle.INITIALIZED);
        m.setCreateCallTime(4000);
        m.setQueryAuthorizations("A,B,C");
        m.setQueryName("TestQuery");
        m.setDocRanges(300);
        m.setNextCount(300);
        m.setSeekCount(300);
        m.setUser(DnUtils.getShortName(ALLOWED_CALLER.subjectDN()));
        m.setUserDN(ALLOWED_CALLER.subjectDN());
        m.addPrediction(new BaseQueryMetric.Prediction("PredictionTest", 200.0));
    }
    
    public static String createQueryId() {
        StringBuilder sb = new StringBuilder();
        sb.append(RandomStringUtils.randomNumeric(4));
        sb.append("-");
        sb.append(RandomStringUtils.randomNumeric(4));
        sb.append("-");
        sb.append(RandomStringUtils.randomNumeric(4));
        sb.append("-");
        sb.append(RandomStringUtils.randomNumeric(4));
        return sb.toString();
    }
    
    protected HttpEntity createRequestEntity(DatawaveUserDetails trustedUser, DatawaveUserDetails jwtUser, Object body) throws JsonProcessingException {
        
        HttpHeaders headers = new HttpHeaders();
        if (this.jwtTokenHandler != null && jwtUser != null) {
            String token = jwtTokenHandler.createTokenFromUsers(jwtUser.getUsername(), jwtUser.getProxiedUsers());
            headers.add("Authorization", "Bearer " + token);
        }
        if (trustedUser != null) {
            headers.add(ProxiedEntityX509Filter.SUBJECT_DN_HEADER, trustedUser.getPrimaryUser().getDn().subjectDN());
            headers.add(ProxiedEntityX509Filter.ISSUER_DN_HEADER, trustedUser.getPrimaryUser().getDn().issuerDN());
        }
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        if (body == null) {
            return new HttpEntity<>(null, headers);
        } else {
            return new HttpEntity<>(objectMapper.writeValueAsString(body), headers);
        }
    }
    
    public static void assertEquals(BaseQueryMetric m1, BaseQueryMetric m2) {
        assertEquals("", m1, m2);
    }
    
    /*
     * This method compares the fields of BaseQueryMetric one by one so that the discrepancy is obvious It also rounds all Date objects to
     */
    public static void assertEquals(String message, BaseQueryMetric m1, BaseQueryMetric m2) {
        if (null == m2) {
            Assertions.fail(message + ": actual metric is null");
        } else if (m1 == m2) {
            return;
        } else {
            if (message == null || message.isEmpty()) {
                message = "";
            } else {
                message = message + ": ";
            }
            Assertions.assertTrue(assertObjectsEqual(m1.getQueryId(), m2.getQueryId()), message + "queryId");
            Assertions.assertTrue(assertObjectsEqual(m1.getQueryType(), m2.getQueryType()), message + "queryType");
            Assertions.assertTrue(assertObjectsEqual(m1.getQueryAuthorizations(), m2.getQueryAuthorizations()), message + "queryAuthorizations");
            Assertions.assertTrue(assertObjectsEqual(m1.getColumnVisibility(), m2.getColumnVisibility()), message + "columnVisibility");
            Assertions.assertEquals(m1.getMarkings(), m2.getMarkings(), message + "markings");
            Assertions.assertTrue(assertObjectsEqual(m1.getBeginDate(), m2.getBeginDate()), message + "beginDate");
            Assertions.assertTrue(assertObjectsEqual(m1.getEndDate(), m2.getEndDate()), message + "endDate");
            Assertions.assertEquals(m1.getCreateDate(), m2.getCreateDate(), message + "createDate");
            Assertions.assertEquals(m1.getSetupTime(), m2.getSetupTime(), message + "setupTime");
            Assertions.assertEquals(m1.getCreateCallTime(), m2.getCreateCallTime(), message + "createCallTime");
            Assertions.assertTrue(assertObjectsEqual(m1.getUser(), m2.getUser()), message + "user");
            Assertions.assertTrue(assertObjectsEqual(m1.getUserDN(), m2.getUserDN()), message + "userDN");
            Assertions.assertTrue(assertObjectsEqual(m1.getQuery(), m2.getQuery()), message + "query");
            Assertions.assertTrue(assertObjectsEqual(m1.getQueryLogic(), m2.getQueryLogic()), message + "queryLogic");
            Assertions.assertTrue(assertObjectsEqual(m1.getQueryName(), m2.getQueryName()), message + "queryName");
            Assertions.assertTrue(assertObjectsEqual(m1.getParameters(), m2.getParameters()), message + "parameters");
            Assertions.assertTrue(assertObjectsEqual(m1.getHost(), m2.getHost()), message + "host");
            Assertions.assertTrue(assertObjectsEqual(m1.getPageTimes(), m2.getPageTimes()), message + "pageTimes");
            Assertions.assertTrue(assertObjectsEqual(m1.getProxyServers(), m2.getProxyServers()), message + "proxyServers");
            Assertions.assertTrue(assertObjectsEqual(m1.getLifecycle(), m2.getLifecycle()), message + "lifecycle");
            Assertions.assertTrue(assertObjectsEqual(m1.getErrorMessage(), m2.getErrorMessage()), message + "errorMessage");
            Assertions.assertTrue(assertObjectsEqual(m1.getErrorCode(), m2.getErrorCode()), message + "errorCode");
            Assertions.assertEquals(m1.getSourceCount(), m2.getSourceCount(), message + "sourceCount");
            Assertions.assertEquals(m1.getNextCount(), m2.getNextCount(), message + "nextCount");
            Assertions.assertEquals(m1.getSeekCount(), m2.getSeekCount(), message + "seekCount");
            Assertions.assertEquals(m1.getYieldCount(), m2.getYieldCount(), message + "yieldCount");
            Assertions.assertEquals(m1.getDocRanges(), m2.getDocRanges(), message + "docRanges");
            Assertions.assertEquals(m1.getFiRanges(), m2.getFiRanges(), message + "fiRanges");
            Assertions.assertTrue(assertObjectsEqual(m1.getPlan(), m2.getPlan()), message + "plan");
            Assertions.assertEquals(m1.getLoginTime(), m2.getLoginTime(), message + "loginTime");
            Assertions.assertTrue(assertObjectsEqual(m1.getPredictions(), m2.getPredictions()), message + "predictions");
            Assertions.assertEquals(m1.getVersionMap(), m2.getVersionMap(), message + "versionMap");
        }
    }
    
    public static boolean assertObjectsEqual(Object o1, Object o2) {
        if (o1 == null && o2 == null) {
            return true;
        } else if (o1 == null && o2 != null) {
            return false;
        } else if (o1 != null && o2 == null) {
            return false;
        } else if (o1.getClass() != o2.getClass()) {
            return false;
        } else if (o1 instanceof Date) {
            return datesEqual((Date) o1, (Date) o2);
        } else {
            return o1.equals(o2);
        }
    }
    
    public static boolean datesEqual(Date d1, Date d2) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        return sdf.format(d1).equals(sdf.format(d2));
    }
    
    public static String formatDate(Date d) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        return sdf.format(d);
    }
    
    protected Collection<String> getAllAccumuloEntries() {
        List<String> entries = new ArrayList<>();
        List<String> tables = new ArrayList<>();
        tables.add(queryMetricHandlerProperties.getShardTableName());
        tables.add(queryMetricHandlerProperties.getIndexTableName());
        tables.add(queryMetricHandlerProperties.getReverseIndexTableName());
        tables.forEach(t -> {
            entries.addAll(getAccumuloEntryStrings(t));
        });
        return entries;
    }
    
    protected Collection<String> getMetadataEntries() {
        return getAccumuloEntryStrings(queryMetricHandlerProperties.getMetadataTableName());
    }
    
    protected Collection<String> getAccumuloEntryStrings(String table) {
        List<String> entryStrings = new ArrayList<>();
        try {
            Collection<Map.Entry<Key,Value>> entries = getAccumuloEntries(connector, table, this.auths);
            for (Map.Entry<Key,Value> e : entries) {
                entryStrings.add(table + " -> " + e.getKey());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return entryStrings;
    }
    
    protected void printAllAccumuloEntries() {
        getAllAccumuloEntries().forEach(s -> System.out.println(s));
    }
    
    public static Collection<Map.Entry<Key,Value>> getAccumuloEntries(Connector connector, String table, Collection<String> authorizations) throws Exception {
        Collection<Map.Entry<Key,Value>> entries = new ArrayList<>();
        String[] authArray = new String[authorizations.size()];
        authorizations.toArray(authArray);
        Authorizations auths = new Authorizations(authArray);
        try (BatchScanner bs = connector.createBatchScanner(table, auths, 1)) {
            bs.setRanges(Collections.singletonList(new Range()));
            final Iterator<Map.Entry<Key,Value>> itr = bs.iterator();
            while (itr.hasNext()) {
                entries.add(itr.next());
            }
        }
        return entries;
    }
    
    public static void deleteAccumuloEntries(Connector connector, List<String> tables, Collection<String> authorizations) {
        try {
            String[] authArray = new String[authorizations.size()];
            authorizations.toArray(authArray);
            tables.forEach(t -> {
                Authorizations auths = new Authorizations(authArray);
                try (BatchDeleter bd = connector.createBatchDeleter(t, auths, 1, new BatchWriterConfig())) {
                    bd.setRanges(Collections.singletonList(new Range()));
                    bd.delete();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    protected void ensureDataStored(Cache incomingCache, String queryId) {
        long now = System.currentTimeMillis();
        int writeDelaySeconds = 1000;
        boolean found = false;
        IMap<Object,Object> hzCache = ((IMap<Object,Object>) incomingCache.getNativeCache());
        while (!found && System.currentTimeMillis() < (now + (1000 * (writeDelaySeconds + 1)))) {
            found = hzCache.containsKey(queryId);
            if (!found) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {}
            }
        }
    }
    
    protected void ensureDataWritten(Cache incomingCache, Cache lastWrittenCache, String queryId) {
        long now = System.currentTimeMillis();
        Config config = ((HazelcastCacheManager) cacheManager).getHazelcastInstance().getConfig();
        MapStoreConfig mapStoreConfig = config.getMapConfig(incomingCache.getName()).getMapStoreConfig();
        int writeDelaySeconds = Math.min(mapStoreConfig.getWriteDelaySeconds(), 1000);
        boolean found = false;
        IMap<Object,Object> hzCache = ((IMap<Object,Object>) lastWrittenCache.getNativeCache());
        while (!found && System.currentTimeMillis() < (now + (1000 * (writeDelaySeconds + 1)))) {
            found = hzCache.containsKey(queryId);
            if (!found) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {}
            }
        }
    }
    
    @Configuration
    @Profile("MessageRouting")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class MessagingTestConfiguration {
        @Primary
        @Bean
        public QueryMetricSupplier testQueryMetricSource(@Lazy QueryMetricOperations queryMetricOperations) {
            return new QueryMetricSupplier() {
                @Override
                public boolean send(Message<QueryMetricUpdate> queryMetricUpdate) {
                    queryMetricOperations.handleEvent(queryMetricUpdate.getPayload());
                    return true;
                }
            };
        }
    }
}
