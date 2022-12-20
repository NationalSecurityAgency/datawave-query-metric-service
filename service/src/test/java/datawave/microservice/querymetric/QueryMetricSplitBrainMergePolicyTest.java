package datawave.microservice.querymetric;

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.hazelcast.config.Config;
import org.junit.After;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static com.hazelcast.util.ExceptionUtil.rethrow;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryMetricSplitBrainMergePolicyTest", "QueryMetricTest", "hazelcast-writethrough"})
public class QueryMetricSplitBrainMergePolicyTest extends QueryMetricTestBase {
    
    @Autowired
    private Config config;
    
    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }
    
    // override cleanup method in QueryMetricTestBase since we terminated that Hazelcast instance
    @After
    public void cleanup() {
        deleteAccumuloEntries(this.accumuloClient, this.tables, this.auths);
    }
    
    @Test
    public void testAllPagesMerged() {
        String mapName = HazelcastUtils.randomMapName();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(newConfig(QueryMetricSplitBrainMergePolicy.class.getName(), mapName));
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(newConfig(QueryMetricSplitBrainMergePolicy.class.getName(), mapName));
        
        HazelcastUtils.warmUpPartition(h1);
        HazelcastUtils.warmUpPartition(h2);
        
        CountDownLatch memberRemovedLatch = new CountDownLatch(1);
        TestMemberShipListener memberShipListener = new TestMemberShipListener(memberRemovedLatch);
        h2.getCluster().addMembershipListener(memberShipListener);
        
        CountDownLatch mergeBlockingLatch = new CountDownLatch(1);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1, mergeBlockingLatch);
        h2.getLifecycleService().addLifecycleListener(lifeCycleListener);
        
        HazelcastUtils.closeConnectionBetween(h1, h2);
        
        HazelcastUtils.assertOpenEventually(memberRemovedLatch);
        HazelcastUtils.assertClusterSizeEventually(1, h1);
        HazelcastUtils.assertClusterSizeEventually(1, h2);
        
        IMap<Object,Object> map1 = h1.getMap(mapName);
        IMap<Object,Object> map2 = h2.getMap(mapName);
        
        BaseQueryMetric metric1 = createMetric();
        BaseQueryMetric.PageMetric pm1 = newPageMetric();
        BaseQueryMetric.PageMetric pm2 = newPageMetric();
        BaseQueryMetric.PageMetric pm3 = newPageMetric();
        BaseQueryMetric.PageMetric pm4 = newPageMetric();
        BaseQueryMetric.PageMetric pm5 = newPageMetric();
        
        map1.put("key1", new QueryMetricUpdateHolder(metric1, QueryMetricType.COMPLETE));
        // prevent updating at the same time
        HazelcastUtils.sleepAtLeastMillis(1000);
        metric1.addPageMetric(pm1);
        metric1.addPageMetric(pm2);
        map2.put("key1", new QueryMetricUpdateHolder(metric1, QueryMetricType.COMPLETE));
        BaseQueryMetric metric2 = createMetric();
        metric2.addPageMetric(pm3);
        metric2.addPageMetric(pm4);
        map2.put("key2", new QueryMetricUpdateHolder(metric2, QueryMetricType.COMPLETE));
        // prevent updating at the same time
        HazelcastUtils.sleepAtLeastMillis(1000);
        metric2.addPageMetric(pm5);
        map1.put("key2", new QueryMetricUpdateHolder(metric2, QueryMetricType.COMPLETE));
        
        // allow merge process to continue
        mergeBlockingLatch.countDown();
        
        HazelcastUtils.assertOpenEventually(lifeCycleListener.mergeFinishedLatch);
        HazelcastUtils.assertClusterSizeEventually(2, h1, h2);
        
        IMap<Object,Object> mapTest = h1.getMap(mapName);
        Assert.assertEquals(2, ((QueryMetricUpdateHolder) mapTest.get("key1")).getMetric().getNumPages());
        Assert.assertEquals(3, ((QueryMetricUpdateHolder) mapTest.get("key2")).getMetric().getNumPages());
    }
    
    @Test
    public void testFieldsUpdated() {
        String mapName = HazelcastUtils.randomMapName();
        Config config = newConfig(QueryMetricSplitBrainMergePolicy.class.getName(), mapName);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        
        HazelcastUtils.warmUpPartition(h1);
        HazelcastUtils.warmUpPartition(h2);
        
        CountDownLatch memberRemovedLatch = new CountDownLatch(1);
        TestMemberShipListener memberShipListener = new TestMemberShipListener(memberRemovedLatch);
        h2.getCluster().addMembershipListener(memberShipListener);
        
        CountDownLatch mergeBlockingLatch = new CountDownLatch(1);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1, mergeBlockingLatch);
        h2.getLifecycleService().addLifecycleListener(lifeCycleListener);
        
        HazelcastUtils.closeConnectionBetween(h1, h2);
        
        HazelcastUtils.assertOpenEventually(memberRemovedLatch);
        HazelcastUtils.assertClusterSizeEventually(1, h1);
        HazelcastUtils.assertClusterSizeEventually(1, h2);
        
        IMap<Object,Object> map1 = h1.getMap(mapName);
        String key = HazelcastUtils.generateKeyOwnedBy(h1);
        Calendar updateTime = Calendar.getInstance();
        long time1 = updateTime.getTimeInMillis();
        updateTime.add(Calendar.SECOND, 1000);
        long time2 = updateTime.getTimeInMillis();
        
        BaseQueryMetric metric1 = createMetric();
        metric1.setLifecycle(BaseQueryMetric.Lifecycle.DEFINED);
        metric1.setLastUpdated(new Date(time1));
        map1.put(key, new QueryMetricUpdateHolder(metric1, QueryMetricType.COMPLETE));
        
        IMap<Object,Object> map2 = h2.getMap(mapName);
        metric1.setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
        metric1.setLastUpdated(new Date(time2));
        map2.put(key, new QueryMetricUpdateHolder(metric1, QueryMetricType.COMPLETE));
        
        // allow merge process to continue
        mergeBlockingLatch.countDown();
        
        HazelcastUtils.assertOpenEventually(lifeCycleListener.mergeFinishedLatch);
        HazelcastUtils.assertClusterSizeEventually(2, h1, h2);
        
        IMap<Object,Object> mapTest = h2.getMap(mapName);
        Assert.assertNotNull(mapTest.get(key));
        BaseQueryMetric mergedMetric = ((QueryMetricUpdateHolder) mapTest.get(key)).getMetric();
        
        Assert.assertEquals(BaseQueryMetric.Lifecycle.CLOSED, mergedMetric.getLifecycle());
        Assert.assertEquals(time2, mergedMetric.getLastUpdated().getTime());
    }
    
    private BaseQueryMetric.PageMetric newPageMetric() {
        String uuid = UUID.randomUUID().toString();
        return new BaseQueryMetric.PageMetric("localhost", uuid, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000);
    }
    
    private Config newConfig(String mergePolicy, String mapName) {
        Config config = getConfig().setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5")
                        .setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        
        config.getGroupConfig().setName(HazelcastUtils.generateRandomString(10));
        
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();
        mergePolicyConfig.setPolicy(mergePolicy);
        config.getMapConfig(mapName).setMergePolicyConfig(mergePolicyConfig);
        return config;
    }
    
    private class TestLifeCycleListener implements LifecycleListener {
        
        final CountDownLatch mergeFinishedLatch;
        final CountDownLatch mergeBlockingLatch;
        
        TestLifeCycleListener(int countdown, CountDownLatch mergeBlockingLatch) {
            this.mergeFinishedLatch = new CountDownLatch(countdown);
            this.mergeBlockingLatch = mergeBlockingLatch;
        }
        
        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleEvent.LifecycleState.MERGING) {
                try {
                    this.mergeBlockingLatch.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw rethrow(e);
                }
            } else if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                this.mergeFinishedLatch.countDown();
            }
        }
    }
    
    private class TestMemberShipListener implements MembershipListener {
        
        final private CountDownLatch memberRemovedLatch;
        
        TestMemberShipListener(CountDownLatch memberRemovedLatch) {
            this.memberRemovedLatch = memberRemovedLatch;
        }
        
        @Override
        public void memberAdded(MembershipEvent membershipEvent) {}
        
        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            this.memberRemovedLatch.countDown();
        }
        
        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {}
    }
    
    private Config getConfig() {
        return this.config;
    }
}
