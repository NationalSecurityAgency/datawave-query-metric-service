package datawave.microservice.querymetric;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.ingest.protobuf.Uid;
import datawave.marking.MarkingFunctions;
import datawave.microservice.authorization.preauth.ProxiedEntityX509Filter;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.querymetric.config.QueryMetricClientProperties;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.handler.AccumuloClientTracking;
import datawave.microservice.querymetric.handler.QueryMetricCombiner;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
import datawave.webservice.common.connection.AccumuloClientPool;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import javax.inject.Named;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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
    protected @Qualifier("warehouse") AccumuloClientPool accumuloClientPool;
    
    @Autowired
    protected QueryMetricHandlerProperties queryMetricHandlerProperties;
    
    @Autowired
    protected QueryMetricFactory queryMetricFactory;
    
    @Autowired
    protected QueryMetricClient client;
    
    @Autowired
    private QueryMetricClientProperties queryMetricClientProperties;
    
    protected Cache incomingQueryMetricsCache;
    protected Cache lastWrittenQueryMetricCache;
    
    @LocalServerPort
    protected int webServicePort;
    
    protected RestTemplate restTemplate;
    protected ProxiedUserDetails adminUser;
    protected ProxiedUserDetails nonAdminUser;
    protected static CacheManager staticCacheManager;
    protected Map<String,String> metricMarkings;
    protected List<String> tables;
    protected Collection<String> auths;
    protected AccumuloClient accumuloClient;
    
    @AfterClass
    public static void afterClass() {
        ((HazelcastCacheManager) staticCacheManager).getHazelcastInstance().shutdown();
    }
    
    @Before
    public void setup() {
        this.queryMetricClientProperties.setPort(webServicePort);
        this.restTemplate = restTemplateBuilder.build(RestTemplate.class);
        this.auths = Arrays.asList("PUBLIC", "A", "B", "C");
        this.metricMarkings = new HashMap<>();
        this.metricMarkings.put(MarkingFunctions.Default.COLUMN_VISIBILITY, "A&C");
        Collection<String> roles = Arrays.asList("Administrator");
        DatawaveUser adminDWUser = new DatawaveUser(ALLOWED_CALLER, USER, null, auths, roles, null, System.currentTimeMillis());
        DatawaveUser nonAdminDWUser = new DatawaveUser(ALLOWED_CALLER, USER, null, auths, null, null, System.currentTimeMillis());
        this.adminUser = new ProxiedUserDetails(Collections.singleton(adminDWUser), adminDWUser.getCreationTime());
        this.nonAdminUser = new ProxiedUserDetails(Collections.singleton(nonAdminDWUser), nonAdminDWUser.getCreationTime());
        QueryMetricTestBase.staticCacheManager = cacheManager;
        this.incomingQueryMetricsCache = cacheManager.getCache(INCOMING_METRICS);
        this.lastWrittenQueryMetricCache = cacheManager.getCache(LAST_WRITTEN_METRICS);
        this.shardTableQueryMetricHandler.verifyTables();
        BaseQueryMetric m = createMetric();
        // this is to ensure that the QueryMetrics_m table
        // is populated so that queries work properly
        try {
            this.shardTableQueryMetricHandler.writeMetric(m, Collections.emptyList(), m.getCreateDate().getTime(), false);
            this.shardTableQueryMetricHandler.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.tables = new ArrayList<>();
        this.tables.add(queryMetricHandlerProperties.getIndexTableName());
        this.tables.add(queryMetricHandlerProperties.getReverseIndexTableName());
        this.tables.add(queryMetricHandlerProperties.getShardTableName());
        try {
            Map<String,String> trackingMap = AccumuloClientTracking.getTrackingMap(Thread.currentThread().getStackTrace());
            this.accumuloClient = this.accumuloClientPool.borrowObject(trackingMap);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        deleteAccumuloEntries(this.accumuloClient, this.tables, this.auths);
        Assert.assertTrue("metadata table empty", getMetadataEntries().size() > 0);
        SimpleModule baseQueryMetricDeserializer = new SimpleModule(BaseQueryMetricListResponse.class.getName());
        baseQueryMetricDeserializer.addAbstractTypeMapping(BaseQueryMetricListResponse.class, QueryMetricListResponse.class);
        this.objectMapper.registerModule(baseQueryMetricDeserializer);
    }
    
    @After
    public void cleanup() {
        deleteAccumuloEntries(this.accumuloClient, this.tables, this.auths);
        this.incomingQueryMetricsCache.clear();
        this.lastWrittenQueryMetricCache.clear();
        this.accumuloClientPool.returnObject(this.accumuloClient);
    }
    
    protected BaseQueryMetric createMetric() {
        return createMetric(createQueryId());
    }
    
    protected BaseQueryMetric createMetric(String queryId) {
        BaseQueryMetric m = this.queryMetricFactory.createMetric();
        populateMetric(m, queryId);
        return m;
    }
    
    protected void populateMetric(BaseQueryMetric m, String queryId) {
        long now = System.currentTimeMillis();
        Date nowDate = new Date(now);
        m.setQueryId(queryId);
        m.setMarkings(this.metricMarkings);
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
    
    protected HttpEntity createRequestEntity(ProxiedUserDetails trustedUser, ProxiedUserDetails jwtUser, Object body) throws JsonProcessingException {
        
        HttpHeaders headers = new HttpHeaders();
        if (this.jwtTokenHandler != null && jwtUser != null) {
            String token = this.jwtTokenHandler.createTokenFromUsers(jwtUser.getUsername(), jwtUser.getProxiedUsers());
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
            return new HttpEntity<>(this.objectMapper.writeValueAsString(body), headers);
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
            Assert.fail(message + ": actual metric is null");
        } else if (m1 == m2) {
            return;
        } else {
            if (message == null || message.isEmpty()) {
                message = "";
            } else {
                message = message + ": ";
            }
            Assert.assertTrue(message + "queryId", assertObjectsEqual(m1.getQueryId(), m2.getQueryId()));
            Assert.assertTrue(message + "queryType", assertObjectsEqual(m1.getQueryType(), m2.getQueryType()));
            Assert.assertTrue(message + "queryAuthorizations", assertObjectsEqual(m1.getQueryAuthorizations(), m2.getQueryAuthorizations()));
            Assert.assertTrue(message + "columnVisibility", assertObjectsEqual(m1.getColumnVisibility(), m2.getColumnVisibility()));
            Assert.assertEquals(message + "markings", m1.getMarkings(), m2.getMarkings());
            Assert.assertTrue(message + "beginDate", assertObjectsEqual(m1.getBeginDate(), m2.getBeginDate()));
            Assert.assertTrue(message + "endDate", assertObjectsEqual(m1.getEndDate(), m2.getEndDate()));
            Assert.assertEquals(message + "createDate", m1.getCreateDate(), m2.getCreateDate());
            Assert.assertEquals(message + "setupTime", m1.getSetupTime(), m2.getSetupTime());
            Assert.assertEquals(message + "createCallTime", m1.getCreateCallTime(), m2.getCreateCallTime());
            Assert.assertTrue(message + "user", assertObjectsEqual(m1.getUser(), m2.getUser()));
            Assert.assertTrue(message + "userDN", assertObjectsEqual(m1.getUserDN(), m2.getUserDN()));
            Assert.assertTrue(message + "query", assertObjectsEqual(m1.getQuery(), m2.getQuery()));
            Assert.assertTrue(message + "queryLogic", assertObjectsEqual(m1.getQueryLogic(), m2.getQueryLogic()));
            Assert.assertTrue(message + "queryName", assertObjectsEqual(m1.getQueryName(), m2.getQueryName()));
            Assert.assertTrue(message + "parameters", assertObjectsEqual(m1.getParameters(), m2.getParameters()));
            Assert.assertTrue(message + "host", assertObjectsEqual(m1.getHost(), m2.getHost()));
            Assert.assertTrue(message + "pageTimes", assertObjectsEqual(m1.getPageTimes(), m2.getPageTimes()));
            Assert.assertTrue(message + "proxyServers", assertObjectsEqual(m1.getProxyServers(), m2.getProxyServers()));
            Assert.assertTrue(message + "lifecycle", assertObjectsEqual(m1.getLifecycle(), m2.getLifecycle()));
            Assert.assertTrue(message + "errorMessage", assertObjectsEqual(m1.getErrorMessage(), m2.getErrorMessage()));
            Assert.assertTrue(message + "errorCode", assertObjectsEqual(m1.getErrorCode(), m2.getErrorCode()));
            Assert.assertEquals(message + "sourceCount", m1.getSourceCount(), m2.getSourceCount());
            Assert.assertEquals(message + "nextCount", m1.getNextCount(), m2.getNextCount());
            Assert.assertEquals(message + "seekCount", m1.getSeekCount(), m2.getSeekCount());
            Assert.assertEquals(message + "yieldCount", m1.getYieldCount(), m2.getYieldCount());
            Assert.assertEquals(message + "docRanges", m1.getDocRanges(), m2.getDocRanges());
            Assert.assertEquals(message + "fiRanges", m1.getFiRanges(), m2.getFiRanges());
            Assert.assertTrue(message + "plan", assertObjectsEqual(m1.getPlan(), m2.getPlan()));
            Assert.assertEquals(message + "loginTime", m1.getLoginTime(), m2.getLoginTime());
            Assert.assertTrue(message + "predictions", assertObjectsEqual(m1.getPredictions(), m2.getPredictions()));
            Assert.assertEquals(message + "versionMap", m1.getVersionMap(), m2.getVersionMap());
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
        tables.add(this.queryMetricHandlerProperties.getShardTableName());
        tables.add(this.queryMetricHandlerProperties.getIndexTableName());
        tables.add(this.queryMetricHandlerProperties.getReverseIndexTableName());
        tables.forEach(t -> {
            entries.addAll(getAccumuloEntryStrings(t));
        });
        return entries;
    }
    
    protected Collection<String> getMetadataEntries() {
        return getAccumuloEntryStrings(this.queryMetricHandlerProperties.getMetadataTableName());
    }
    
    protected Collection<String> getAccumuloEntryStrings(String table) {
        List<String> entryStrings = new ArrayList<>();
        try {
            Collection<Map.Entry<Key,Value>> entries = getAccumuloEntries(this.accumuloClient, table, this.auths);
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
    
    public static Collection<Map.Entry<Key,Value>> getAccumuloEntries(AccumuloClient accumuloClient, String table, Collection<String> authorizations)
                    throws Exception {
        Collection<Map.Entry<Key,Value>> entries = new ArrayList<>();
        String[] authArray = new String[authorizations.size()];
        authorizations.toArray(authArray);
        Authorizations auths = new Authorizations(authArray);
        try (BatchScanner bs = accumuloClient.createBatchScanner(table, auths, 1)) {
            bs.setRanges(Collections.singletonList(new Range()));
            final Iterator<Map.Entry<Key,Value>> itr = bs.iterator();
            while (itr.hasNext()) {
                entries.add(itr.next());
            }
        }
        return entries;
    }
    
    public static void deleteAccumuloEntries(AccumuloClient accumuloClient, List<String> tables, Collection<String> authorizations) {
        try {
            String[] authArray = new String[authorizations.size()];
            authorizations.toArray(authArray);
            tables.forEach(t -> {
                Authorizations auths = new Authorizations(authArray);
                try (BatchDeleter bd = accumuloClient.createBatchDeleter(t, auths, 1, new BatchWriterConfig())) {
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
        Config config = ((HazelcastCacheManager) this.cacheManager).getHazelcastInstance().getConfig();
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
    
    public Collection<Map.Entry<Key,Value>> getEventEntriesFromAccumulo(String queryId) {
        Collection<Map.Entry<Key,Value>> entries = new ArrayList<>();
        try {
            String indexTable = this.queryMetricHandlerProperties.getIndexTableName();
            String shardTable = this.queryMetricHandlerProperties.getShardTableName();
            Collection<Map.Entry<Key,Value>> indexEntries = getAccumuloEntries(this.accumuloClient, indexTable, this.auths);
            Key key = null;
            Value value = null;
            for (Map.Entry<Key,Value> e : indexEntries) {
                Key k = e.getKey();
                if (k.getRow().toString().equals(queryId) && k.getColumnFamily().toString().equals("QUERY_ID")) {
                    key = k;
                    value = e.getValue();
                    break;
                }
            }
            String uid = null;
            if (value != null) {
                Uid.List l = Uid.List.parseFrom(value.get());
                if (l.getUIDCount() > 0) {
                    uid = l.getUID(0);
                }
                if (uid != null) {
                    String[] split = key.getColumnQualifier().toString().split("\0");
                    if (split.length > 0) {
                        Text eventRow = new Text(split[0]);
                        Text eventColFam = new Text("querymetrics\0" + uid);
                        Collection<Map.Entry<Key,Value>> shardEntries = getAccumuloEntries(this.accumuloClient, shardTable, this.auths);
                        for (Map.Entry<Key,Value> e : shardEntries) {
                            if (e.getKey().getRow().equals(eventRow) && e.getKey().getColumnFamily().equals(eventColFam)) {
                                entries.add(e);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return entries;
    }
    
    public void printEventEntriesFromAccumulo(String queryId) {
        Collection<Map.Entry<Key,Value>> entries = getEventEntriesFromAccumulo(queryId);
        for (Map.Entry<Key,Value> e : entries) {
            System.out.println(e.getKey().toString());
        }
    }
    
    public void assertNoDuplicateFields(String queryId) {
        Collection<Map.Entry<Key,Value>> entries = getEventEntriesFromAccumulo(queryId);
        Set<String> fields = new HashSet<>();
        Set<String> duplicateFields = new HashSet<>();
        for (Map.Entry<Key,Value> e : entries) {
            Key k = e.getKey();
            String[] split = k.getColumnQualifier().toString().split("\0");
            if (split.length > 0) {
                String f = split[0];
                if (!fields.contains(f)) {
                    fields.add(f);
                } else {
                    duplicateFields.add(f);
                }
            }
        }
        
        if (!duplicateFields.isEmpty()) {
            for (Map.Entry<Key,Value> e : entries) {
                System.out.println(e.getKey().toString());
            }
            Assert.fail("Duplicate field values found for:" + duplicateFields);
        }
    }
}
