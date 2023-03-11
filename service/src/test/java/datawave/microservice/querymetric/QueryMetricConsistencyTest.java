package datawave.microservice.querymetric;

import com.google.common.collect.Multimap;
import datawave.microservice.querymetric.handler.ContentQueryMetricsIngestHelper;
import datawave.microservice.querymetric.persistence.AccumuloMapStore;
import datawave.util.StringUtils;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryMetricConsistencyTest", "QueryMetricTest", "MessageRouting", "hazelcast-writethrough"})
public class QueryMetricConsistencyTest extends QueryMetricTestBase {
    
    @Autowired
    AccumuloMapStore mapStore;
    
    @BeforeEach
    public void setup() {
        super.setup();
    }
    
    @AfterEach
    public void cleanup() {
        super.cleanup();
    }
    
    @Test
    public void PageMetricTest() throws Exception {
        int port = webServicePort;
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        UriComponents metricUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(port).path(String.format(getMetricsUrl, queryId))
                        .build();
        HttpEntity metricRequestEntity = createRequestEntity(null, adminUser, null);
        
        int numPages = 10;
        for (int i = 0; i < numPages; i++) {
            long now = System.currentTimeMillis();
            m.addPageTime("localhost", 1000, 1000, now - 1000, now);
            // @formatter:off
            client.submit(new QueryMetricClient.Request.Builder()
                    .withMetric(m)
                    .withMetricType(QueryMetricType.COMPLETE)
                    .withUser(adminUser)
                    .build());
            // @formatter:on
            ResponseEntity<BaseQueryMetricListResponse> metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity,
                            BaseQueryMetricListResponse.class);
            assertEquals(1, metricResponse.getBody().getNumResults());
            BaseQueryMetric returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
            assertEquals(i + 1, returnedMetric.getPageTimes().size());
            metricAssertEquals(m, returnedMetric);
        }
    }
    
    @Test
    public void OutOfOrderLifecycleTest() throws Exception {
        int port = webServicePort;
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        UriComponents metricUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(port).path(String.format(getMetricsUrl, queryId))
                        .build();
        m.setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build());
        // @formatter:on
        HttpEntity metricRequestEntity = createRequestEntity(null, adminUser, null);
        ResponseEntity<BaseQueryMetricListResponse> metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity,
                        BaseQueryMetricListResponse.class);
        
        assertEquals(1, metricResponse.getBody().getNumResults());
        BaseQueryMetric returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
        assertEquals(BaseQueryMetric.Lifecycle.CLOSED, returnedMetric.getLifecycle(), "lifecycle incorrect");
        metricAssertEquals(m, returnedMetric);
        
        // send an update with out-of-sequence lifecycle
        m = createMetric(queryId);
        m.setLifecycle(BaseQueryMetric.Lifecycle.INITIALIZED);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build());
        // @formatter:on
        metricRequestEntity = createRequestEntity(null, adminUser, null);
        metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity, BaseQueryMetricListResponse.class);
        
        assertEquals(1, metricResponse.getBody().getNumResults());
        returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
        // metric should have been updated without backtracking on the lifecycle
        assertEquals(BaseQueryMetric.Lifecycle.CLOSED, returnedMetric.getLifecycle(), "lifecycle incorrect");
    }
    
    @Test
    public void DistributedUpdateTest() throws Exception {
        int port = webServicePort;
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
                .withUser(adminUser)
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
                .withUser(adminUser)
                .build());
        // @formatter:on
        HttpEntity metricRequestEntity = createRequestEntity(null, adminUser, null);
        ResponseEntity<BaseQueryMetricListResponse> metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity,
                        BaseQueryMetricListResponse.class);
        
        assertEquals(1, metricResponse.getBody().getNumResults());
        BaseQueryMetric returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
        assertEquals(formatDate(new Date(now - 1000)), formatDate(returnedMetric.getCreateDate()), "create date should be the earlier of the two values");
        assertEquals(formatDate(new Date(now)), formatDate(returnedMetric.getLastUpdated()), "last updated should only increase");
        assertEquals(200, returnedMetric.getSourceCount(), "source count should be additive");
        assertEquals(200, returnedMetric.getNextCount(), "next count should be additive");
        assertEquals(200, returnedMetric.getSeekCount(), "seek count should be additive");
        assertEquals(200, returnedMetric.getYieldCount(), "yield count should be additive");
        assertEquals(200, returnedMetric.getDocRanges(), "doc ranges count should be additive");
        assertEquals(200, returnedMetric.getFiRanges(), "fi ranges should be additive");
        long lastPageNumReturned = queryMetricCombiner.getLastPageNumber(returnedMetric);
        assertEquals(2, lastPageNumReturned, "distributed update should append pages");
        
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
                .withUser(adminUser)
                .build());
        // @formatter:on
        metricRequestEntity = createRequestEntity(null, adminUser, null);
        metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity, BaseQueryMetricListResponse.class);
        
        assertEquals(1, metricResponse.getBody().getNumResults());
        returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
        assertEquals(formatDate(new Date(now + 1000)), formatDate(returnedMetric.getLastUpdated()), "last updated should only increase");
        assertEquals(1000, returnedMetric.getSourceCount(), "latest source count should be used");
        assertEquals(1000, returnedMetric.getNextCount(), "latest next count should be used");
        assertEquals(1000, returnedMetric.getSeekCount(), "latest seek count should be used");
        assertEquals(1000, returnedMetric.getYieldCount(), "latest yield count should be used");
        assertEquals(1000, returnedMetric.getDocRanges(), "latest doc ranges count should be used");
        assertEquals(1000, returnedMetric.getFiRanges(), "latest fi ranges should be used");
    }
    
    @Test
    public void ToMetricTest() {
        
        ContentQueryMetricsIngestHelper.HelperDelegate<QueryMetric> helper = new ContentQueryMetricsIngestHelper.HelperDelegate<>();
        QueryMetric queryMetric = (QueryMetric) createMetric();
        Multimap<String,String> fieldsToWrite = helper.getEventFieldsToWrite(queryMetric);
        
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
        BaseQueryMetric newMetric = shardTableQueryMetricHandler.toMetric(event);
        metricAssertEquals("metrics are not equal", queryMetric, newMetric);
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
        BaseQueryMetric combinedMetric = shardTableQueryMetricHandler.combineMetrics(storedQueryMetric, updatedQueryMetric, QueryMetricType.COMPLETE);
        metricAssertEquals("metric should not change", storedQueryMetricCopy, storedQueryMetric);
        metricAssertEquals("metric should not change", updatedQueryMetricCopy, updatedQueryMetricCopy);
        assertEquals(BaseQueryMetric.Lifecycle.CLOSED, combinedMetric.getLifecycle());
        assertEquals(2, combinedMetric.getNumPages());
    }
    
    @Test
    public void MetricUpdateTest() throws Exception {
        QueryMetric storedQueryMetric = (QueryMetric) createMetric();
        QueryMetric updatedQueryMetric = (QueryMetric) storedQueryMetric.duplicate();
        updatedQueryMetric.setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
        updatedQueryMetric.setNumResults(2000);
        updatedQueryMetric.setNumUpdates(200);
        updatedQueryMetric.setDocRanges(400);
        updatedQueryMetric.setNextCount(400);
        updatedQueryMetric.setSeekCount(400);
        
        Date now = new Date();
        shardTableQueryMetricHandler.writeMetric(storedQueryMetric, Collections.singletonList(storedQueryMetric), now, false);
        shardTableQueryMetricHandler.writeMetric(updatedQueryMetric, Collections.singletonList(storedQueryMetric), now, true);
        
        Collection<Map.Entry<Key,Value>> entries = QueryMetricTestBase.getAccumuloEntries(connector, queryMetricHandlerProperties.getShardTableName(),
                        this.auths);
        Map<String,String> updatedFields = new HashMap();
        updatedFields.put("NUM_UPDATES", "200");
        updatedFields.put("NUM_RESULTS", "2000");
        updatedFields.put("LIFECYCLE", "CLOSED");
        updatedFields.put("DOC_RANGES", "400");
        updatedFields.put("NEXT_COUNT", "400");
        updatedFields.put("SEEK_COUNT", "400");
        assertFalse(entries.isEmpty(), "There should be entries in Accumulo");
        for (Map.Entry<Key,Value> e : entries) {
            if (e.getKey().getColumnFamily().toString().startsWith("querymetrics")) {
                String fieldName = fieldSplit(e, 0);
                if (updatedFields.containsKey(fieldName)) {
                    fail(fieldName + " should have been deleted");
                }
            }
        }
        
        shardTableQueryMetricHandler.writeMetric(updatedQueryMetric, Collections.singletonList(storedQueryMetric), now, false);
        entries = QueryMetricTestBase.getAccumuloEntries(connector, queryMetricHandlerProperties.getShardTableName(), this.auths);
        assertFalse(entries.isEmpty(), "There should be entries in Accumulo");
        for (Map.Entry<Key,Value> e : entries) {
            if (e.getKey().getColumnFamily().toString().startsWith("querymetrics")) {
                String fieldName = fieldSplit(e, 0);
                String fieldValue = fieldSplit(e, 1);
                if (updatedFields.containsKey(fieldName)) {
                    assertEquals(updatedFields.get(fieldName), fieldValue, fieldName + " should have been updated");
                }
            }
        }
    }
    
    @Test
    public void DuplicateAccumuloEntryTest() throws Exception {
        String queryId = createQueryId();
        QueryMetric storedQueryMetric = (QueryMetric) createMetric(queryId);
        QueryMetric updatedQueryMetric = (QueryMetric) storedQueryMetric.duplicate();
        updatedQueryMetric.setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
        updatedQueryMetric.setNumResults(2000);
        updatedQueryMetric.setNumUpdates(200);
        updatedQueryMetric.setDocRanges(400);
        updatedQueryMetric.setNextCount(400);
        updatedQueryMetric.setSeekCount(400);
        
        mapStore.store(queryId, new QueryMetricUpdateHolder(storedQueryMetric, QueryMetricType.COMPLETE));
        QueryMetricUpdateHolder lastWrittenMetricUpdate = lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdateHolder.class);
        assertEquals(storedQueryMetric, lastWrittenMetricUpdate.getMetric());
        
        mapStore.store(queryId, new QueryMetricUpdateHolder(updatedQueryMetric, QueryMetricType.COMPLETE));
        lastWrittenMetricUpdate = lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdateHolder.class);
        // all fields that were changed should be reflected in the updated metric
        assertEquals(updatedQueryMetric, lastWrittenMetricUpdate.getMetric());
        
        Collection<Map.Entry<Key,Value>> entries = QueryMetricTestBase.getAccumuloEntries(connector, queryMetricHandlerProperties.getShardTableName(),
                        this.auths);
        
        assertFalse(entries.isEmpty(), "There should be entries in Accumulo");
        Set<String> foundFields = new HashSet<>();
        for (Map.Entry<Key,Value> e : entries) {
            if (e.getKey().getColumnFamily().toString().startsWith("querymetrics")) {
                String fieldName = fieldSplit(e, 0);
                if (foundFields.contains(fieldName)) {
                    fail("duplicate field " + fieldName + " found in Accumulo");
                } else {
                    foundFields.add(fieldName);
                }
            }
        }
    }
    
    private String fieldSplit(Map.Entry<Key,Value> entry, int part) {
        String cq = entry.getKey().getColumnQualifier().toString();
        return StringUtils.split(cq, "\u0000")[part];
    }
}
