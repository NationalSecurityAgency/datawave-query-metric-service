package datawave.microservice.querymetric;

import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

public class QueryMetricSplitBrainMergePolicyTest {
    
    private Logger log = LoggerFactory.getLogger(TestMemberShipListener.class);
    
    private final static QueryMetricFactory queryMetricFactory = new QueryMetricFactoryImpl();
    
    @Test
    public void testAllPagesMerged() {
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
        
        HazelcastUtils.assertClusterSizeEventually(2, h1, h2);
        
        Assertions.assertEquals(2, h1.getCluster().getMembers().size());
        Assertions.assertEquals(2, h2.getCluster().getMembers().size());
        
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
        
        map1.put("key1", new QueryMetricUpdate(metric1, QueryMetricType.COMPLETE));
        // prevent updating at the same time
        HazelcastUtils.sleepAtLeastMillis(1000);
        metric1.addPageMetric(pm1);
        metric1.addPageMetric(pm2);
        map2.put("key1", new QueryMetricUpdate(metric1, QueryMetricType.COMPLETE));
        BaseQueryMetric metric2 = createMetric();
        metric2.addPageMetric(pm3);
        metric2.addPageMetric(pm4);
        map2.put("key2", new QueryMetricUpdate(metric2, QueryMetricType.COMPLETE));
        // prevent updating at the same time
        HazelcastUtils.sleepAtLeastMillis(1000);
        metric2.addPageMetric(pm5);
        map1.put("key2", new QueryMetricUpdate(metric2, QueryMetricType.COMPLETE));
        
        // allow merge process to continue
        mergeBlockingLatch.countDown();
        
        HazelcastUtils.assertOpenEventually(lifeCycleListener.mergeFinishedLatch);
        HazelcastUtils.assertClusterSizeEventually(2, h1, h2);
        
        IMap<Object,Object> mapTest = h1.getMap(mapName);
        Assertions.assertEquals(2, ((QueryMetricUpdate) mapTest.get("key1")).getMetric().getNumPages());
        Assertions.assertEquals(3, ((QueryMetricUpdate) mapTest.get("key2")).getMetric().getNumPages());
        
        h1.shutdown();
        h2.shutdown();
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
        
        HazelcastUtils.assertClusterSizeEventually(2, h1, h2);
        
        Assertions.assertEquals(2, h1.getCluster().getMembers().size());
        Assertions.assertEquals(2, h2.getCluster().getMembers().size());
        
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
        map1.put(key, new QueryMetricUpdate(metric1, QueryMetricType.COMPLETE));
        
        IMap<Object,Object> map2 = h2.getMap(mapName);
        metric1.setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
        metric1.setLastUpdated(new Date(time2));
        map2.put(key, new QueryMetricUpdate(metric1, QueryMetricType.COMPLETE));
        
        // allow merge process to continue
        mergeBlockingLatch.countDown();
        
        HazelcastUtils.assertOpenEventually(lifeCycleListener.mergeFinishedLatch);
        HazelcastUtils.assertClusterSizeEventually(2, h1, h2);
        
        IMap<Object,Object> mapTest = h2.getMap(mapName);
        Assertions.assertNotNull(mapTest.get(key));
        BaseQueryMetric mergedMetric = ((QueryMetricUpdate) mapTest.get(key)).getMetric();
        
        Assertions.assertEquals(BaseQueryMetric.Lifecycle.CLOSED, mergedMetric.getLifecycle());
        Assertions.assertEquals(time2, mergedMetric.getLastUpdated().getTime());
        
        h1.shutdown();
        h2.shutdown();
    }
    
    private BaseQueryMetric createMetric() {
        return QueryMetricTestBase.createMetric(queryMetricFactory);
    }
    
    private BaseQueryMetric.PageMetric newPageMetric() {
        String uuid = UUID.randomUUID().toString();
        return new BaseQueryMetric.PageMetric("localhost", uuid, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000);
    }
    
    private Config newConfig(String mergePolicy, String mapName) {
        Config config = new Config().setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5")
                        .setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        
        config.setClusterName(HazelcastUtils.generateRandomString(10));
        
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();
        mergePolicyConfig.setPolicy(mergePolicy);
        config.getMapConfig(mapName).setMergePolicyConfig(mergePolicyConfig);
        config.getMapConfig(mapName).setPerEntryStatsEnabled(true);
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
                    mergeBlockingLatch.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw rethrow(e);
                }
            } else if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                mergeFinishedLatch.countDown();
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
            memberRemovedLatch.countDown();
        }
    }
}
