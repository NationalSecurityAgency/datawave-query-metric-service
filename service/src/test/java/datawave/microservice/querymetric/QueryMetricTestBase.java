package datawave.microservice.querymetric;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.marking.MarkingFunctions;
import datawave.microservice.authorization.preauth.ProxiedEntityX509Filter;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static datawave.microservice.querymetric.config.HazelcastServerConfiguration.INCOMING_METRICS;
import static datawave.microservice.querymetric.config.HazelcastServerConfiguration.LAST_WRITTEN_METRICS;
import static datawave.security.authorization.DatawaveUser.UserType.USER;

public class QueryMetricTestBase {
    
    protected Logger log = LoggerFactory.getLogger(getClass());
    
    protected static final SubjectIssuerDNPair ALLOWED_CALLER = SubjectIssuerDNPair.of("cn=test a. user, ou=example developers, o=example corp, c=us",
                    "cn=example corp ca, o=example corp, c=us");
    
    protected static final String updateMetricUrl = "/querymetric/v1/updateMetric";
    protected static final String updateMetricsUrl = "/querymetric/v1/updateMetrics";
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
    protected CacheManager cacheManager;
    
    @Autowired
    protected @Qualifier("warehouse") Connector connector;
    
    @Autowired
    protected QueryMetricHandlerProperties queryMetricHandlerProperties;
    
    @Autowired
    protected QueryMetricFactory queryMetricFactory;
    
    protected Cache incomingQueryMetricsCache;
    protected Cache lastWrittenQueryMetricCache;
    
    @LocalServerPort
    protected int webServicePort;
    
    protected RestTemplate restTemplate;
    protected ProxiedUserDetails adminUser;
    protected ProxiedUserDetails nonAdminUser;
    protected static boolean isHazelCast;
    protected static CacheManager staticCacheManager;
    protected Map<String,String> metricMarkings;
    
    @AfterClass
    public static void afterClass() {
        if (isHazelCast) {
            ((HazelcastCacheManager) staticCacheManager).getHazelcastInstance().shutdown();
        }
    }
    
    @Before
    public void setup() {
        this.restTemplate = restTemplateBuilder.build(RestTemplate.class);
        Collection<String> auths = Arrays.asList("PUBLIC", "A", "B", "C");
        this.metricMarkings = new HashMap<>();
        this.metricMarkings.put(MarkingFunctions.Default.COLUMN_VISIBILITY, "A&C");
        Collection<String> roles = Arrays.asList("Administrator");
        DatawaveUser adminDWUser = new DatawaveUser(ALLOWED_CALLER, USER, null, auths, roles, null, System.currentTimeMillis());
        DatawaveUser nonAdminDWUser = new DatawaveUser(ALLOWED_CALLER, USER, null, auths, null, null, System.currentTimeMillis());
        this.adminUser = new ProxiedUserDetails(Collections.singleton(adminDWUser), adminDWUser.getCreationTime());
        this.nonAdminUser = new ProxiedUserDetails(Collections.singleton(nonAdminDWUser), nonAdminDWUser.getCreationTime());
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
        deleteAllAccumuloEntries();
        Assert.assertTrue("metadata table empty", getMetadataEntries().size() > 0);
        SimpleModule baseQueryMetricDeserializer = new SimpleModule(BaseQueryMetricListResponse.class.getName());
        baseQueryMetricDeserializer.addAbstractTypeMapping(BaseQueryMetricListResponse.class, QueryMetricListResponse.class);
        objectMapper.registerModule(baseQueryMetricDeserializer);
    }
    
    @After
    public void cleanup() {
        deleteAllAccumuloEntries();
        this.incomingQueryMetricsCache.clear();
        this.lastWrittenQueryMetricCache.clear();
    }
    
    protected BaseQueryMetric createMetric() {
        return createMetric(createQueryId());
    }
    
    protected BaseQueryMetric createMetric(String queryId) {
        BaseQueryMetric m = queryMetricFactory.createMetric();
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
        m.setUser(DnUtils.getShortName(ALLOWED_CALLER.subjectDN()));
        m.setUserDN(ALLOWED_CALLER.subjectDN());
    }
    
    protected String createQueryId() {
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
            String token = jwtTokenHandler.createTokenFromUsers(jwtUser.getUsername(), jwtUser.getProxiedUsers());
            headers.add("Authorization", "Bearer " + token);
        }
        if (trustedUser != null) {
            headers.add(ProxiedEntityX509Filter.SUBJECT_DN_HEADER, trustedUser.getPrimaryUser().getDn().subjectDN());
            headers.add(ProxiedEntityX509Filter.ISSUER_DN_HEADER, trustedUser.getPrimaryUser().getDn().issuerDN());
        }
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        
        return new HttpEntity<>(objectMapper.writeValueAsString(body), headers);
    }
    
    protected void assertEquals(BaseQueryMetric m1, BaseQueryMetric m2) {
        assertEquals("", m1, m2);
    }
    
    /*
     * This method compares the fields of BaseQueryMetric one by one so that the discrepancy is obvious It also rounds all Date objects to
     */
    protected void assertEquals(String message, BaseQueryMetric m1, BaseQueryMetric m2) {
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
        }
    }
    
    protected boolean assertObjectsEqual(Object o1, Object o2) {
        if (o1 == null && o2 == null) {
            return true;
        } else if (o1 == null && o2 != null) {
            return false;
        } else if (o1 != null && o2 == null) {
            return false;
        } else if (o1.getClass() != o2.getClass()) {
            return false;
        } else if (o1 instanceof Date) {
            long t1 = ((((Date) o1).getTime()) / 1000) * 1000;
            long t2 = ((((Date) o2).getTime()) / 1000) * 1000;
            return t1 == t2;
        } else {
            return o1.equals(o2);
        }
    }
    
    protected Collection<String> getAllAccumuloEntries() {
        List<String> entries = new ArrayList<>();
        List<String> tables = new ArrayList<>();
        tables.add(queryMetricHandlerProperties.getShardTableName());
        tables.add(queryMetricHandlerProperties.getIndexTableName());
        tables.add(queryMetricHandlerProperties.getReverseIndexTableName());
        tables.forEach(t -> {
            entries.addAll(getAccumuloEntries(t));
        });
        return entries;
    }
    
    protected Collection<String> getMetadataEntries() {
        return getAccumuloEntries(queryMetricHandlerProperties.getMetadataTableName());
    }
    
    protected Collection<String> getAccumuloEntries(String table) {
        List<String> entries = new ArrayList<>();
        try {
            Authorizations auths = new Authorizations("PUBLIC");
            try (BatchScanner bs = this.connector.createBatchScanner(table, auths, 1)) {
                bs.setRanges(Collections.singletonList(new Range()));
                final Iterator<Map.Entry<Key,Value>> itr = bs.iterator();
                while (itr.hasNext()) {
                    entries.add(table + " -> " + itr.next().getKey());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return entries;
    }
    
    protected void printAllAccumuloEntries() {
        getAllAccumuloEntries().forEach(s -> System.out.println(s));
    }
    
    protected void deleteAllAccumuloEntries() {
        List<String> tables = new ArrayList<>();
        tables.add(queryMetricHandlerProperties.getShardTableName());
        tables.add(queryMetricHandlerProperties.getIndexTableName());
        tables.add(queryMetricHandlerProperties.getReverseIndexTableName());
        try {
            tables.forEach(t -> {
                Authorizations auths = new Authorizations("PUBLIC");
                try (BatchDeleter bd = this.connector.createBatchDeleter(t, auths, 1, new BatchWriterConfig())) {
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
    
    protected void waitForWriteBehind(Cache incomingCache, Cache lastWrittenCache, String queryId) {
        long now = System.currentTimeMillis();
        Config config = ((HazelcastCacheManager) cacheManager).getHazelcastInstance().getConfig();
        MapStoreConfig mapStoreConfig = config.getMapConfig(incomingCache.getName()).getMapStoreConfig();
        int writeDelaySeconds = mapStoreConfig.getWriteDelaySeconds();
        // only wait on value if this is a write-behind cache
        if (writeDelaySeconds > 0) {
            boolean found = false;
            MapProxyImpl hzCache = ((MapProxyImpl) lastWrittenCache.getNativeCache());
            while (!found && System.currentTimeMillis() < (now + (1000 * (writeDelaySeconds + 1)))) {
                found = hzCache.containsKey(queryId);
                if (!found) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {}
                }
            }
        }
    }
}
