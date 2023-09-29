package datawave.microservice.querymetric;

import com.google.common.collect.Multimap;
import datawave.data.hash.UID;
import datawave.microservice.querymetric.handler.ContentQueryMetricsIngestHelper;
import datawave.microservice.querymetric.persistence.AccumuloMapStore;
import datawave.util.StringUtils;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryMetricConsistencyTest", "QueryMetricTest", "hazelcast-writethrough"})
public class QueryMetricConsistencyTest extends QueryMetricTestBase {
    
    @Autowired
    AccumuloMapStore mapStore;
    
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void cleanup() {
        super.cleanup();
    }
    
    @Test
    public void PageMetricTest() throws Exception {
        int port = this.webServicePort;
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        UriComponents metricUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(port).path(String.format(getMetricsUrl, queryId))
                        .build();
        HttpEntity metricRequestEntity = createRequestEntity(null, this.adminUser, null);
        
        int numPages = 10;
        for (int i = 0; i < numPages; i++) {
            long now = System.currentTimeMillis();
            m.addPageTime("localhost", 1000, 1000, now - 1000, now);
            // @formatter:off
            client.submit(new QueryMetricClient.Request.Builder()
                    .withMetric(m)
                    .withMetricType(QueryMetricType.COMPLETE)
                    .withUser(this.adminUser)
                    .build());
            // @formatter:on
            ResponseEntity<BaseQueryMetricListResponse> metricResponse = this.restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity,
                            BaseQueryMetricListResponse.class);
            Assert.assertEquals(1, metricResponse.getBody().getNumResults());
            BaseQueryMetric returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
            Assert.assertEquals(i + 1, returnedMetric.getPageTimes().size());
            assertEquals(m, returnedMetric);
        }
        assertNoDuplicateFields(queryId);
    }
    
    public void updateRangeCounts(Map<String,Integer> shardCounts, Map<String,Integer> documentCounts, String subplan, Range range) {
        Key key = range.getStartKey();
        String cf = key.getColumnFamily().toString();
        if (cf.length() > 0) {
            if (documentCounts.containsKey(subplan)) {
                documentCounts.put(subplan, documentCounts.get(subplan) + 1);
            } else {
                documentCounts.put(subplan, 1);
            }
        } else {
            if (shardCounts.containsKey(subplan)) {
                shardCounts.put(subplan, shardCounts.get(subplan) + 1);
            } else {
                shardCounts.put(subplan, 1);
            }
        }
    }
    
    @Test
    public void SubPlanTest() throws Exception {
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        Map<String,Integer> shardCounts = new HashMap<>();
        Map<String,Integer> documentCounts = new HashMap<>();
        List<String> plans = new ArrayList<>();
        plans.add("F1 == 'value1' || F2 == 'value2'");
        plans.add("F3 == 'value3' || F4 == 'value4'");
        plans.add("F2 == 'value2' || F3 == 'value3'");
        plans.add("F1 == 'value1' || F6 == 'value6'");
        plans.add("F1 == 'value1' || F5 == 'value5'");
        Random r = new Random(System.currentTimeMillis());
        
        Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String date = sdf.format(now);
        int numShardSubplans = 10;
        for (int i = 0; i < numShardSubplans; i++) {
            String shard = date + "_" + i;
            Key begin = new Key(shard);
            Key end = begin.followingKey(PartialKey.ROW);
            String subplan = plans.get(r.nextInt(plans.size()));
            updateRangeCounts(shardCounts, documentCounts, subplan, new Range(begin, end));
        }
        int numDocSubplans = 10;
        for (int i = 0; i < numDocSubplans; i++) {
            String shard = date + "_" + i;
            String uid = UID.builder().newId().toString();
            Key begin = new Key(shard, "datatype\0" + uid);
            Key end = begin.followingKey(PartialKey.ROW_COLFAM);
            String subplan = plans.get(r.nextInt(plans.size()));
            updateRangeCounts(shardCounts, documentCounts, subplan, new Range(begin, end));
        }
        for (String p : plans) {
            Integer shardCount = shardCounts.getOrDefault(p, 0);
            Integer documentCount = documentCounts.getOrDefault(p, 0);
            m.addSubPlan(p, Arrays.asList(shardCount, documentCount));
        }
        
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(this.adminUser)
                .build());
        // @formatter:on
        this.ensureDataWritten(incomingQueryMetricsCache, lastWrittenQueryMetricCache, queryId);
        BaseQueryMetric storedMetric = shardTableQueryMetricHandler.getQueryMetric(queryId);
        Assert.assertEquals(m.getSubPlans().size(), storedMetric.getSubPlans().size());
        assertEquals(m, storedMetric);
    }
    
    @Test
    public void OutOfOrderLifecycleTest() throws Exception {
        int port = this.webServicePort;
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        UriComponents metricUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(port).path(String.format(getMetricsUrl, queryId))
                        .build();
        m.setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(this.adminUser)
                .build());
        // @formatter:on
        HttpEntity metricRequestEntity = createRequestEntity(null, this.adminUser, null);
        ResponseEntity<BaseQueryMetricListResponse> metricResponse = this.restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity,
                        BaseQueryMetricListResponse.class);
        
        Assert.assertEquals(1, metricResponse.getBody().getNumResults());
        BaseQueryMetric returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
        Assert.assertEquals("lifecycle incorrect", BaseQueryMetric.Lifecycle.CLOSED, returnedMetric.getLifecycle());
        assertEquals(m, returnedMetric);
        
        // send an update with out-of-sequence lifecycle
        m = createMetric(queryId);
        m.setLifecycle(BaseQueryMetric.Lifecycle.INITIALIZED);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(this.adminUser)
                .build());
        // @formatter:on
        metricRequestEntity = createRequestEntity(null, this.adminUser, null);
        metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity, BaseQueryMetricListResponse.class);
        
        Assert.assertEquals(1, metricResponse.getBody().getNumResults());
        returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
        // metric should have been updated without backtracking on the lifecycle
        Assert.assertEquals("lifecycle incorrect", BaseQueryMetric.Lifecycle.CLOSED, returnedMetric.getLifecycle());
        assertNoDuplicateFields(queryId);
    }
    
    @Test
    public void DistributedUpdateTest() throws Exception {
        int port = this.webServicePort;
        String queryId = createQueryId();
        UriComponents metricUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(port).path(String.format(getMetricsUrl, queryId))
                        .build();
        
        long now = System.currentTimeMillis();
        BaseQueryMetric m = createMetric(queryId);
        m.setCreateDate(new Date(now));
        m.setLastUpdated(new Date(now));
        m.setSourceCount(100);
        m.setNextCount(100);
        m.setSeekCount(100);
        m.setYieldCount(100);
        m.setDocRanges(100);
        m.setFiRanges(100);
        BaseQueryMetric.PageMetric pm = new BaseQueryMetric.PageMetric("localhost", 1000, 1000, 1000, 1000, 2000, 0, 0, -1);
        pm.setPageNumber(1);
        m.addPageMetric(pm);
        m.setLifecycle(BaseQueryMetric.Lifecycle.INITIALIZED);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.DISTRIBUTED)
                .withUser(this.adminUser)
                .build());
        // @formatter:on
        m = createMetric(queryId);
        m.setCreateDate(new Date(now - 1000));
        m.setLastUpdated(new Date(now - 1000));
        m.setSourceCount(100);
        m.setNextCount(100);
        m.setSeekCount(100);
        m.setYieldCount(100);
        m.setDocRanges(100);
        m.setFiRanges(100);
        pm = new BaseQueryMetric.PageMetric("localhost", 1000, 1000, 1000, 1000, 2000, 0, 0, -1);
        pm.setPageNumber(1);
        m.addPageMetric(pm);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.DISTRIBUTED)
                .withUser(this.adminUser)
                .build());
        // @formatter:on
        HttpEntity metricRequestEntity = createRequestEntity(null, this.adminUser, null);
        ResponseEntity<BaseQueryMetricListResponse> metricResponse = this.restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity,
                        BaseQueryMetricListResponse.class);
        
        Assert.assertEquals(1, metricResponse.getBody().getNumResults());
        BaseQueryMetric returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
        Assert.assertEquals("create date should be the first received of the two values", formatDate(new Date(now)),
                        formatDate(returnedMetric.getCreateDate()));
        Assert.assertEquals("last updated should only increase", formatDate(new Date(now)), formatDate(returnedMetric.getLastUpdated()));
        Assert.assertEquals("source count should be additive", 200, returnedMetric.getSourceCount());
        Assert.assertEquals("next count should be additive", 200, returnedMetric.getNextCount());
        Assert.assertEquals("seek count should be additive", 200, returnedMetric.getSeekCount());
        Assert.assertEquals("yield count should be additive", 200, returnedMetric.getYieldCount());
        Assert.assertEquals("doc ranges count should be additive", 200, returnedMetric.getDocRanges());
        Assert.assertEquals("fi ranges should be additive", 200, returnedMetric.getFiRanges());
        long lastPageNumReturned = this.queryMetricCombiner.getLastPageNumber(returnedMetric);
        Assert.assertEquals("distributed update should append pages", 2, lastPageNumReturned);
        
        m.setLastUpdated(new Date(now + 1000));
        m.setSourceCount(1000);
        m.setNextCount(1000);
        m.setSeekCount(1000);
        m.setYieldCount(1000);
        m.setDocRanges(1000);
        m.setFiRanges(1000);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(this.adminUser)
                .build());
        // @formatter:on
        metricRequestEntity = createRequestEntity(null, this.adminUser, null);
        metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity, BaseQueryMetricListResponse.class);
        
        Assert.assertEquals(1, metricResponse.getBody().getNumResults());
        returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
        Assert.assertEquals("last updated should only increase", formatDate(new Date(now + 1000)), formatDate(returnedMetric.getLastUpdated()));
        Assert.assertEquals("latest source count should be used", 1000, returnedMetric.getSourceCount());
        Assert.assertEquals("latest next count should be used", 1000, returnedMetric.getNextCount());
        Assert.assertEquals("latest seek count should be used", 1000, returnedMetric.getSeekCount());
        Assert.assertEquals("latest yield count should be used", 1000, returnedMetric.getYieldCount());
        Assert.assertEquals("latest doc ranges count should be used", 1000, returnedMetric.getDocRanges());
        Assert.assertEquals("latest fi ranges should be used", 1000, returnedMetric.getFiRanges());
        assertNoDuplicateFields(queryId);
    }
    
    @Test
    public void ToMetricTest() {
        
        ContentQueryMetricsIngestHelper.HelperDelegate<QueryMetric> helper = new ContentQueryMetricsIngestHelper.HelperDelegate<>();
        QueryMetric queryMetric = (QueryMetric) createMetric();
        Multimap<String,String> fieldsToWrite = helper.getEventFieldsToWrite(queryMetric, null);
        
        EventBase event = new DefaultEvent();
        long now = System.currentTimeMillis();
        List<FieldBase> fields = new ArrayList<>();
        fieldsToWrite.asMap().forEach((k, set) -> {
            set.forEach(v -> {
                fields.add(new DefaultField(k, "", now, v));
            });
        });
        event.setFields(fields);
        event.setMarkings(queryMetric.getMarkings());
        BaseQueryMetric newMetric = this.shardTableQueryMetricHandler.toMetric(event);
        QueryMetricTestBase.assertEquals("metrics are not equal", queryMetric, newMetric);
    }
    
    @Test
    public void CombineMetricsTest() throws Exception {
        QueryMetric storedQueryMetric = (QueryMetric) createMetric();
        storedQueryMetric.addPageTime(10, 500, 500000, 500000);
        QueryMetric updatedQueryMetric = (QueryMetric) storedQueryMetric.duplicate();
        updatedQueryMetric.addPageTime(100, 1000, 5000, 10000);
        updatedQueryMetric.setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
        
        BaseQueryMetric storedQueryMetricCopy = storedQueryMetric.duplicate();
        BaseQueryMetric updatedQueryMetricCopy = updatedQueryMetric.duplicate();
        BaseQueryMetric combinedMetric = this.shardTableQueryMetricHandler.combineMetrics(storedQueryMetric, updatedQueryMetric, QueryMetricType.COMPLETE);
        QueryMetricTestBase.assertEquals("metric should not change", storedQueryMetricCopy, storedQueryMetric);
        QueryMetricTestBase.assertEquals("metric should not change", updatedQueryMetricCopy, updatedQueryMetricCopy);
        Assert.assertEquals(BaseQueryMetric.Lifecycle.CLOSED, combinedMetric.getLifecycle());
        Assert.assertEquals(2, combinedMetric.getNumPages());
    }
    
    @Test
    public void MetricUpdateTest() throws Exception {
        String queryId = createQueryId();
        QueryMetric storedQueryMetric = (QueryMetric) createMetric(queryId);
        QueryMetric updatedQueryMetric = (QueryMetric) storedQueryMetric.duplicate();
        updatedQueryMetric.setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
        updatedQueryMetric.setNumResults(2000);
        updatedQueryMetric.setDocRanges(400);
        updatedQueryMetric.setNextCount(400);
        updatedQueryMetric.setSeekCount(400);
        
        Date now = new Date();
        this.shardTableQueryMetricHandler.writeMetric(storedQueryMetric, Collections.emptyList(), now.getTime(), false);
        this.shardTableQueryMetricHandler.writeMetric(updatedQueryMetric, Collections.singletonList(storedQueryMetric), now.getTime(), true);
        
        Collection<Map.Entry<Key,Value>> entries = getEventEntriesFromAccumulo(queryId);
        Map<String,String> updatedFields = new HashMap();
        updatedFields.put("NUM_RESULTS", "2000");
        updatedFields.put("LIFECYCLE", "CLOSED");
        updatedFields.put("DOC_RANGES", "400");
        updatedFields.put("NEXT_COUNT", "400");
        updatedFields.put("SEEK_COUNT", "400");
        Assert.assertFalse("There should be entries in Accumulo", entries.isEmpty());
        for (Map.Entry<Key,Value> e : entries) {
            if (e.getKey().getColumnFamily().toString().startsWith("querymetrics")) {
                String fieldName = fieldSplit(e, 0);
                if (updatedFields.containsKey(fieldName)) {
                    Assert.fail(fieldName + " should have been deleted");
                }
            }
        }
        
        shardTableQueryMetricHandler.writeMetric(updatedQueryMetric, Collections.singletonList(storedQueryMetric), now.getTime(), false);
        entries = getEventEntriesFromAccumulo(queryId);
        Assert.assertFalse("There should be entries in Accumulo", entries.isEmpty());
        for (Map.Entry<Key,Value> e : entries) {
            if (e.getKey().getColumnFamily().toString().startsWith("querymetrics")) {
                String fieldName = fieldSplit(e, 0);
                String fieldValue = fieldSplit(e, 1);
                if (updatedFields.containsKey(fieldName)) {
                    Assert.assertEquals(fieldName + " should have been updated", updatedFields.get(fieldName), fieldValue);
                }
            }
        }
        assertNoDuplicateFields(storedQueryMetric.getQueryId());
    }
    
    @Test
    public void DuplicateAccumuloEntryTest() throws Exception {
        String queryId = createQueryId();
        QueryMetric storedQueryMetric = (QueryMetric) createMetric(queryId);
        QueryMetric updatedQueryMetric = (QueryMetric) storedQueryMetric.duplicate();
        updatedQueryMetric.setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
        updatedQueryMetric.setNumResults(2000);
        updatedQueryMetric.setDocRanges(400);
        updatedQueryMetric.setNextCount(400);
        updatedQueryMetric.setSeekCount(400);
        
        QueryMetricUpdateHolder holder = new QueryMetricUpdateHolder(storedQueryMetric, QueryMetricType.COMPLETE);
        mapStore.store(queryId, holder);
        QueryMetricUpdateHolder lastWrittenMetricUpdate = this.lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdateHolder.class);
        assertEquals(storedQueryMetric, lastWrittenMetricUpdate.getMetric());
        
        holder.setMetric(updatedQueryMetric);
        mapStore.store(queryId, holder);
        lastWrittenMetricUpdate = this.lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdateHolder.class);
        // all fields that were changed should be reflected in the updated metric
        assertEquals(updatedQueryMetric, lastWrittenMetricUpdate.getMetric());
        
        Collection<Map.Entry<Key,Value>> entries = getEventEntriesFromAccumulo(queryId);
        Assert.assertFalse("There should be entries in Accumulo", entries.isEmpty());
        
        assertNoDuplicateFields(queryId);
    }
    
    private String fieldSplit(Map.Entry<Key,Value> entry, int part) {
        String cq = entry.getKey().getColumnQualifier().toString();
        return StringUtils.split(cq, "\u0000")[part];
    }
}
