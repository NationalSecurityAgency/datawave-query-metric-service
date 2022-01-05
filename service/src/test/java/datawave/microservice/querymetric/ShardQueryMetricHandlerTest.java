package datawave.microservice.querymetric;

import com.google.common.collect.Multimap;
import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.handler.ContentQueryMetricsIngestHelper;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"QueryMetricTest"})
public class ShardQueryMetricHandlerTest {
    
    protected static final SubjectIssuerDNPair ALLOWED_CALLER = SubjectIssuerDNPair.of("cn=test a. user, ou=example developers, o=example corp, c=us",
                    "cn=example corp ca, o=example corp, c=us");
    @Autowired
    protected ShardTableQueryMetricHandler shardTableQueryMetricHandler;
    
    @Test
    public void testToMetric() {
        
        ContentQueryMetricsIngestHelper.HelperDelegate<QueryMetric> helper = new ContentQueryMetricsIngestHelper.HelperDelegate<>();
        QueryMetric queryMetric = new QueryMetric();
        populateMetric(queryMetric, UUID.randomUUID().toString());
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
        QueryMetricTestBase.assertEquals("metrics are not equal", queryMetric, newMetric);
    }
    
    protected void populateMetric(BaseQueryMetric m, String queryId) {
        long now = System.currentTimeMillis();
        Date nowDate = new Date(now);
        m.setQueryId(queryId);
        HashMap<String,String> markings = new HashMap();
        markings.put(MarkingFunctions.Default.COLUMN_VISIBILITY, "A&C");
        m.setMarkings(markings);
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
        m.addPrediction(new BaseQueryMetric.Prediction("PredictionTest", 200.0));
        m.addPageTime(10, 500, 500000, 500000);
    }
    
}
