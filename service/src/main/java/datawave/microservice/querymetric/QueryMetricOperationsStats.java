package datawave.microservice.querymetric;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.hazelcast.monitor.LocalMapStats;
import datawave.microservice.querymetric.config.TimelyProperties;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.persistence.AccumuloMapStore;
import datawave.util.timely.TcpClient;
import datawave.util.timely.UdpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MINUTES;

public class QueryMetricOperationsStats {
    
    private Logger log = LoggerFactory.getLogger(getClass());
    private Map<TIMERS,Timer> timerMap = new HashMap<>();
    private Map<METERS,Meter> meterMap = new HashMap<>();
    private TcpClient timelyTcpClient;
    private UdpClient timelyUdpClient;
    
    protected TimelyProperties timelyProperties;
    protected ShardTableQueryMetricHandler handler;
    protected AccumuloMapStore mapStore;
    protected Map<String,Long> hostCountMap = new HashMap<>();
    protected Map<String,Long> userCountMap = new HashMap<>();
    protected Map<String,Long> logicCountMap = new HashMap<>();
    protected List<String> metricsToWrite = Collections.synchronizedList(new ArrayList<>());
    
    public enum TIMERS {
        STORE
    }
    
    public enum METERS {
        REST, MESSAGE
    }
    
    /*
     * Timer Hierarchy
     *
     * REST - receive request and send it out to the message queue MESSAGE - receive message and do all of the following CORRELATE COMBINE STORE ENTRY
     */
    
    public QueryMetricOperationsStats(TimelyProperties timelyProperties, ShardTableQueryMetricHandler handler, AccumuloMapStore mapStore) {
        this.timelyProperties = timelyProperties;
        this.handler = handler;
        this.mapStore = mapStore;
        for (TIMERS name : TIMERS.values()) {
            timerMap.put(name, new Timer(new SlidingTimeWindowArrayReservoir(1, MINUTES)));
        }
        for (METERS name : METERS.values()) {
            meterMap.put(name, new Meter());
        }
        if (this.timelyProperties.getEnabled()) {
            try {
                if (timelyProperties.getProtocol().equals(TimelyProperties.Protocol.TCP)) {
                    this.timelyTcpClient = new TcpClient(timelyProperties.getHost(), timelyProperties.getPort());
                    this.timelyTcpClient.open();
                } else {
                    this.timelyUdpClient = new UdpClient(timelyProperties.getHost(), timelyProperties.getPort());
                    this.timelyUdpClient.open();
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
    
    public Timer getTimer(TIMERS name) {
        return timerMap.get(name);
    }
    
    public Meter getMeter(METERS name) {
        return meterMap.get(name);
    }
    
    public void writeTimelyData() {
        synchronized (this.metricsToWrite) {
            try {
                for (String metric : this.metricsToWrite) {
                    if (timelyProperties.getProtocol().equals(TimelyProperties.Protocol.TCP)) {
                        timelyTcpClient.write(metric);
                    } else {
                        timelyUdpClient.write(metric);
                    }
                }
                this.metricsToWrite.clear();
            } catch (Exception e) {
                log.error("Exception writing metrics to Timely: " + e.getMessage());
                // keep trying to write metrics, but can't store them forever
                if (this.metricsToWrite.size() > 10000) {
                    this.metricsToWrite.clear();
                }
            } finally {
                if (timelyProperties.getProtocol().equals(TimelyProperties.Protocol.TCP)) {
                    timelyTcpClient.flush();
                }
            }
        }
    }
    
    public Map<String,String> getLocalMapStats(LocalMapStats localMapStats) {
        Map<String,String> stats = new LinkedHashMap<>();
        DecimalFormat nFormat = new DecimalFormat("#,##0");
        stats.put("creationTime", formatDate(localMapStats.getCreationTime()));
        stats.put("lastUpdateTime", formatDate(localMapStats.getLastUpdateTime()));
        stats.put("lastAccessTime", formatDate(localMapStats.getLastAccessTime()));
        stats.put("putOperationCount", nFormat.format(localMapStats.getPutOperationCount()));
        stats.put("eventOperationCount", nFormat.format(localMapStats.getEventOperationCount()));
        stats.put("otherOperationCount", nFormat.format(localMapStats.getOtherOperationCount()));
        stats.put("getOperationCount", nFormat.format(localMapStats.getGetOperationCount()));
        stats.put("ownedEntryCount", nFormat.format(localMapStats.getOwnedEntryCount()));
        stats.put("backupCount", nFormat.format(localMapStats.getBackupCount()));
        stats.put("backupEntryCount", nFormat.format(localMapStats.getBackupEntryCount()));
        stats.put("dirtyEntryCount", nFormat.format(localMapStats.getDirtyEntryCount()));
        return stats;
    }
    
    private String formatDate(long milliseconds) {
        if (milliseconds <= 0) {
            return "";
        } else {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
            return sdf.format(new Date(milliseconds));
        }
    }
    
    public Map<String,String> formatStats(Map<String,Double> stats, boolean useSeparators) {
        DecimalFormat dFormat = useSeparators ? new DecimalFormat("#,##0.00") : new DecimalFormat("#0.00");
        DecimalFormat nFormat = useSeparators ? new DecimalFormat("#,##0") : new DecimalFormat("#0");
        Map<String,String> formattedStats = new LinkedHashMap<>();
        stats.entrySet().forEach(e -> {
            if (e.getKey().contains("Latency")) {
                formattedStats.put(e.getKey(), nFormat.format(e.getValue()));
            } else {
                formattedStats.put(e.getKey(), dFormat.format(e.getValue()));
            }
        });
        return formattedStats;
    }
    
    public Map<String,Double> getServiceStats() {
        Map<String,Double> stats = new LinkedHashMap<>();
        addTimerStats("store", getTimer(TIMERS.STORE), stats);
        addTimerStats("accumulo", mapStore.getWriteTimer(), stats);
        addMeterStats("message", getMeter(METERS.MESSAGE), stats);
        addMeterStats("rest", getMeter(METERS.REST), stats);
        return stats;
    }
    
    public void queueTimelyMetrics(QueryMetricUpdate update) {
        queueTimelyMetrics(update.getMetric());
    }
    
    public void queueTimelyMetrics(BaseQueryMetric queryMetric) {
        if (this.timelyProperties.getEnabled() && queryMetric.getQueryType().equalsIgnoreCase("RunningQuery")) {
            BaseQueryMetric.Lifecycle lifecycle = queryMetric.getLifecycle();
            String host = queryMetric.getHost();
            String user = queryMetric.getUser();
            String logic = queryMetric.getQueryLogic();
            if (lifecycle.equals(BaseQueryMetric.Lifecycle.CLOSED) || lifecycle.equals(BaseQueryMetric.Lifecycle.CANCELLED)) {
                long createDate = queryMetric.getCreateDate().getTime();
                // write ELAPSED_TIME
                this.metricsToWrite.add("put dw.query.metrics.ELAPSED_TIME " + createDate + " " + queryMetric.getElapsedTime() + " HOST=" + host + "\n");
                this.metricsToWrite.add("put dw.query.metrics.ELAPSED_TIME " + createDate + " " + queryMetric.getElapsedTime() + " USER=" + user + "\n");
                this.metricsToWrite
                                .add("put dw.query.metrics.ELAPSED_TIME " + createDate + " " + queryMetric.getElapsedTime() + " QUERY_LOGIC=" + logic + "\n");
                
                // write NUM_RESULTS
                this.metricsToWrite.add("put dw.query.metrics.NUM_RESULTS " + createDate + " " + queryMetric.getNumResults() + " HOST=" + host + "\n");
                this.metricsToWrite.add("put dw.query.metrics.NUM_RESULTS " + createDate + " " + queryMetric.getNumResults() + " USER=" + user + "\n");
                this.metricsToWrite.add("put dw.query.metrics.NUM_RESULTS " + createDate + " " + queryMetric.getNumResults() + " QUERY_LOGIC=" + logic + "\n");
            } else if (lifecycle.equals(BaseQueryMetric.Lifecycle.INITIALIZED)) {
                // aggregate these metrics for later writing to timely
                synchronized (hostCountMap) {
                    Long hostCount = hostCountMap.get(host);
                    hostCountMap.put(host, hostCount == null ? 1l : hostCount + 1);
                    Long userCount = userCountMap.get(user);
                    userCountMap.put(user, userCount == null ? 1l : userCount + 1);
                    Long logicCount = logicCountMap.get(logic);
                    logicCountMap.put(logic, logicCount == null ? 1l : logicCount + 1);
                }
            }
        }
    }
    
    public void logServiceStats() {
        Map<String,Double> stats = getServiceStats();
        Map<String,String> serviceStats = formatStats(stats, true);
        String storeRate1 = serviceStats.get("storeRatePerSec_1_Min_Avg");
        String storeRate5 = serviceStats.get("storeRatePerSec_5_Min_Avg");
        String storeRate15 = serviceStats.get("storeRatePerSec_15_Min_Avg");
        String storeLat = serviceStats.get("storeLatency_Mean");
        log.info("storeMetric rates/sec 1m={} 5m={} 15m={} opLatMs={}", storeRate1, storeRate5, storeRate15, storeLat);
        String accumuloRate1 = serviceStats.get("accumuloRatePerSec_1_Min_Avg");
        String accumuloRate5 = serviceStats.get("accumuloRatePerSec_5_Min_Avg");
        String accumuloRate15 = serviceStats.get("accumuloRatePerSec_15_Min_Avg");
        String accumuloLat = serviceStats.get("accumuloLatency_Mean");
        log.info("accumulo rates/sec 1m={} 5m={} 15m={} opLatMs={}", accumuloRate1, accumuloRate5, accumuloRate15, accumuloLat);
    }
    
    public void queueAggregatedMetricsForTimely() {
        if (this.timelyProperties.getEnabled()) {
            long now = System.currentTimeMillis();
            synchronized (hostCountMap) {
                hostCountMap.entrySet().forEach(entry -> {
                    this.metricsToWrite.add("put dw.query.metrics.COUNT " + now + " " + entry.getValue() + " HOST=" + entry.getKey() + "\n");
                });
                hostCountMap.clear();
                userCountMap.entrySet().forEach(entry -> {
                    this.metricsToWrite.add("put dw.query.metrics.COUNT " + now + " " + entry.getValue() + " USER=" + entry.getKey() + "\n");
                });
                userCountMap.clear();
                logicCountMap.entrySet().forEach(entry -> {
                    this.metricsToWrite.add("put dw.query.metrics.COUNT " + now + " " + entry.getValue() + " QUERY_LOGIC=" + entry.getKey() + "\n");
                });
                logicCountMap.clear();
            }
        }
    }
    
    public void queueServiceStatsForTimely() {
        if (this.timelyProperties.getEnabled()) {
            Map<String,String> tagMap = new LinkedHashMap<>();
            try {
                tagMap.put("host", InetAddress.getLocalHost().getCanonicalHostName());
            } catch (Exception e) {
                
            }
            String tags = tagMap.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(" "));
            long timestamp = System.currentTimeMillis();
            Map<String,String> serviceStats = formatStats(getServiceStats(), false);
            serviceStats.entrySet().forEach(entry -> {
                this.metricsToWrite.add("put microservice.querymetric." + entry.getKey() + " " + timestamp + " " + entry.getValue() + " " + tags + "\n");
            });
        }
    }
    
    private void addTimerStats(String baseName, Timer timer, Map<String,Double> stats) {
        Snapshot snapshot = timer.getSnapshot();
        stats.put(baseName + "Latency_Mean", snapshot.getMean() / 1000000);
        stats.put(baseName + "Latency_Median", snapshot.getMedian() / 1000000);
        stats.put(baseName + "Latency_Max", ((Number) snapshot.getMax()).doubleValue() / 1000000);
        stats.put(baseName + "Latency_Min", ((Number) snapshot.getMin()).doubleValue() / 1000000);
        stats.put(baseName + "Latency_75", snapshot.get75thPercentile() / 1000000);
        stats.put(baseName + "Latency_95", snapshot.get95thPercentile() / 1000000);
        stats.put(baseName + "Latency_99", snapshot.get99thPercentile() / 1000000);
        stats.put(baseName + "Latency_999", snapshot.get999thPercentile() / 1000000);
        stats.put(baseName + "RatePerSec_1_Min_Avg", timer.getOneMinuteRate());
        stats.put(baseName + "RatePerSec_5_Min_Avg", timer.getFiveMinuteRate());
        stats.put(baseName + "RatePerSec_15_Min_Avg", timer.getFifteenMinuteRate());
    }
    
    private void addMeterStats(String baseName, Metered meter, Map<String,Double> stats) {
        stats.put(baseName + "RatePerSec_1_Min_Avg", meter.getOneMinuteRate());
        stats.put(baseName + "RatePerSec_5_Min_Avg", meter.getFiveMinuteRate());
        stats.put(baseName + "RatePerSec_15_Min_Avg", meter.getFifteenMinuteRate());
    }
}
