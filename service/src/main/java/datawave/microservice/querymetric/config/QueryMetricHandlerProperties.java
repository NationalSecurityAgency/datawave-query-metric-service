package datawave.microservice.querymetric.config;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.ingest.table.config.MetadataTableConfigHelper;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.microservice.querymetric.handler.ContentQueryMetricsIngestHelper;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Positive;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@Validated
@ConfigurationProperties(prefix = "datawave.query.metric.handler")
public class QueryMetricHandlerProperties {
    
    @NotBlank
    protected String defaultMetricVisibility;
    @NotBlank
    protected String queryVisibility;
    @NotBlank
    protected String zookeepers;
    @NotBlank
    protected String instanceName;
    @NotBlank
    protected String username;
    @NotBlank
    protected String password;
    protected int accumuloClientPoolSize = 16;
    protected int mapStoreWriteThreads = 16;
    protected int numShards = 10;
    protected String shardTableName = "QueryMetrics_e";
    protected String indexTableName = "QueryMetrics_i";
    protected String reverseIndexTableName = "QueryMetrics_r";
    protected String metadataTableName = "QueryMetrics_m";
    protected String metadataDefaultAuths = "";
    protected boolean metadataTableFrequencyEnabled = true;
    protected boolean createTables = true;
    @Positive
    protected long maxWriteMilliseconds = 60000l;
    @Positive
    protected long maxReadMilliseconds = 60000l;
    protected List<String> fatalErrors = Collections.singletonList("UUID_MISSING");
    protected String dateField = "CREATE_DATE";
    protected String dateFormat = "yyyyMMdd HHmmss.S";
    protected int fieldLengthThreshold = 4049;
    protected boolean enableBloomFilter = false;
    @Positive
    protected int recordWriterMaxMemory = 10000000;
    // TabletServerBatchWriter uses latency / 4 to get a Timer period
    @Min(10)
    protected int recordWriterMaxLatency = 60000;
    protected int recordWriterNumThreads = 4;
    protected String policyEnforcerClass = "datawave.policy.IngestPolicyEnforcer$NoOpIngestPolicyEnforcer";
    protected String baseMaps = "{}";
    
    //@formatter:off
    protected List<String> indexFields = Arrays.asList(
            "AUTHORIZATIONS",
            "BEGIN_DATE",
            "CREATE_CALL_TIME",
            "CREATE_DATE",
            "DOC_RANGES",
            "ELAPSED_TIME",
            "END_DATE",
            "ERROR_CODE",
            "ERROR_MESSAGE",
            "FI_RANGES",
            "HOST",
            "LIFECYCLE",
            "LOGIN_TIME",
            "NEGATIVE_SELECTORS",
            "NEXT_COUNT",
            "NUM_PAGES",
            "NUM_RESULTS",
            "NUM_UPDATES",
            "PARAMETERS",
            "PLAN",
            "POSITIVE_SELECTORS",
            "PROXY_SERVERS",
            "QUERY",
            "QUERY_ID",
            "QUERY_LOGIC",
            "QUERY_NAME",
            "QUERY_TYPE",
            "SEEK_COUNT",
            "SETUP_TIME",
            "SOURCE_COUNT",
            "USER");

    protected List<String> additionalIndexFields = Collections.EMPTY_LIST;

    protected List<String> reverseIndexFields = Arrays.asList(
            "ERROR_CODE",
            "ERROR_MESSAGE",
            "HOST",
            "NEGATIVE_SELECTORS",
            "PARAMETERS",
            "PLAN",
            "POSITIVE_SELECTORS",
            "PROXY_SERVERS",
            "QUERY",
            "QUERY_ID",
            "QUERY_LOGIC",
            "QUERY_NAME",
            "QUERY_TYPE",
            "USER");

    protected List<String> additionalReverseIndexFields = Collections.EMPTY_LIST;

    protected List<String> numericFields = Arrays.asList(
            "CREATE_CALL_TIME",
            "ELAPSED_TIME",
            "LOGIN_TIME",
            "SETUP_TIME",
            "NUM_PAGES",
            "NUM_RESULTS",
            "NUM_UPDATES");

    protected List<String> additionalNumericFields = Collections.EMPTY_LIST;
    //@formatter:on
    
    public Map<String,String> getProperties() {
        
        Map<String,String> p = new HashMap<>();
        p.put("ingest.data.types", "querymetrics");
        // p.put("AccumuloRecordWriter.reader.class", "");
        p.put("AccumuloRecordWriter.zooKeepers", zookeepers);
        p.put("AccumuloRecordWriter.instanceName", instanceName);
        p.put("AccumuloRecordWriter.username", username);
        // encode the password because that's how the AccumuloRecordWriter expects it
        byte[] encodedPassword = Base64.encodeBase64(password.getBytes(Charset.forName("UTF-8")));
        p.put("AccumuloRecordWriter.password", new String(encodedPassword, Charset.forName("UTF-8")));
        p.put("AccumuloRecordWriter.createtables", Boolean.toString(createTables));
        p.put(shardTableName + ".table.config.class", ShardTableConfigHelper.class.getCanonicalName());
        p.put(indexTableName + ".table.config.class", ShardTableConfigHelper.class.getCanonicalName());
        p.put(reverseIndexTableName + ".table.config.class", ShardTableConfigHelper.class.getCanonicalName());
        p.put(metadataTableName + ".table.config.class", MetadataTableConfigHelper.class.getCanonicalName());
        p.put("num.shards", Integer.toString(numShards));
        p.put("sharded.table.names", shardTableName);
        p.put("shard.table.name", shardTableName);
        p.put("shard.global.index.table.name", indexTableName);
        p.put("shard.global.rindex.table.name", reverseIndexTableName);
        p.put("metadata.table.name", metadataTableName);
        p.put("metadata.term.frequency.enabled", Boolean.toString(metadataTableFrequencyEnabled));
        p.put("shard.table.locality.groups", "termfrequency:tf");
        p.put("shard.table.index.bloom.enable", Boolean.toString(enableBloomFilter));
        p.put("data.name", "querymetrics");
        p.put("querymetrics.ingest.fatal.errors", StringUtils.join(fatalErrors, ','));
        p.put("querymetrics.ingest.helper.class", ContentQueryMetricsIngestHelper.class.getCanonicalName());
        p.put("querymetrics.data.category.date", dateField);
        p.put("querymetrics.data.category.date.format", dateFormat);
        p.put("querymetrics.data.separator", ",");
        p.put("querymetrics.data.category.uuid.fields", "QUERY_ID");
        p.put("querymetrics.data.header", "none");
        p.put("querymetrics.data.field.length.threshold", Integer.toString(fieldLengthThreshold));
        Set<String> combinedIndexFields = new TreeSet<>(indexFields);
        combinedIndexFields.addAll(additionalIndexFields);
        p.put("querymetrics.data.category.index", StringUtils.join(combinedIndexFields, ","));
        Set<String> combinedReverseIndexFields = new TreeSet<>(reverseIndexFields);
        combinedReverseIndexFields.addAll(additionalReverseIndexFields);
        p.put("querymetrics.data.category.index.reverse", StringUtils.join(combinedReverseIndexFields, ','));
        p.put("querymetrics.data.category.token.fieldname.designator", "");
        p.put("querymetrics.data.default.type.class", LcNoDiacriticsType.class.getCanonicalName());
        p.put("querymetrics.ingest.policy.enforcer.class", policyEnforcerClass);
        Set<String> combinedNumericFields = new TreeSet<>(numericFields);
        combinedNumericFields.addAll(additionalNumericFields);
        combinedNumericFields.forEach(f -> {
            p.put("querymetrics." + f + ".data.field.type.class", NumberType.class.getCanonicalName());
        });
        p.put("AccumuloRecordWriter.maxmemory", Integer.toString(recordWriterMaxMemory));
        p.put("AccumuloRecordWriter.maxlatency", Integer.toString(recordWriterMaxLatency));
        p.put("AccumuloRecordWriter.writethreads", Integer.toString(recordWriterNumThreads));
        return p;
    }
    
    public void setDefaultMetricVisibility(String defaultMetricVisibility) {
        this.defaultMetricVisibility = defaultMetricVisibility;
    }
    
    public String getDefaultMetricVisibility() {
        return defaultMetricVisibility;
    }
    
    public void setQueryVisibility(String queryVisibility) {
        this.queryVisibility = queryVisibility;
    }
    
    public String getQueryVisibility() {
        return queryVisibility;
    }
    
    public String getZookeepers() {
        return zookeepers;
    }
    
    public void setZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
    }
    
    public String getInstanceName() {
        return instanceName;
    }
    
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }
    
    public String getUsername() {
        return username;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    public int getAccumuloClientPoolSize() {
        return accumuloClientPoolSize;
    }
    
    public void setAccumuloClientPoolSize(int accumuloClientPoolSize) {
        this.accumuloClientPoolSize = accumuloClientPoolSize;
    }
    
    public int getMapStoreWriteThreads() {
        return mapStoreWriteThreads;
    }
    
    public void setMapStoreWriteThreads(int mapStoreWriteThreads) {
        this.mapStoreWriteThreads = mapStoreWriteThreads;
    }
    
    public int getNumShards() {
        return numShards;
    }
    
    public void setNumShards(int numShards) {
        this.numShards = numShards;
    }
    
    public String getShardTableName() {
        return shardTableName;
    }
    
    public void setShardTableName(String shardTableName) {
        this.shardTableName = shardTableName;
    }
    
    public String getIndexTableName() {
        return indexTableName;
    }
    
    public void setIndexTableName(String indexTableName) {
        this.indexTableName = indexTableName;
    }
    
    public String getReverseIndexTableName() {
        return reverseIndexTableName;
    }
    
    public void setReverseIndexTableName(String reverseIndexTableName) {
        this.reverseIndexTableName = reverseIndexTableName;
    }
    
    public String getMetadataTableName() {
        return metadataTableName;
    }
    
    public void setMetadataTableName(String metadataTableName) {
        this.metadataTableName = metadataTableName;
    }
    
    public String getMetadataDefaultAuths() {
        return metadataDefaultAuths;
    }
    
    public void setMetadataDefaultAuths(String metadataDefaultAuths) {
        this.metadataDefaultAuths = metadataDefaultAuths;
    }
    
    public boolean isMetadataTableFrequencyEnabled() {
        return metadataTableFrequencyEnabled;
    }
    
    public void setMetadataTableFrequencyEnabled(boolean metadataTableFrequencyEnabled) {
        this.metadataTableFrequencyEnabled = metadataTableFrequencyEnabled;
    }
    
    public boolean isCreateTables() {
        return createTables;
    }
    
    public void setCreateTables(boolean createTables) {
        this.createTables = createTables;
    }
    
    public long getMaxReadMilliseconds() {
        return maxReadMilliseconds;
    }
    
    public void setMaxReadMilliseconds(long maxReadMilliseconds) {
        this.maxReadMilliseconds = maxReadMilliseconds;
    }
    
    public long getMaxWriteMilliseconds() {
        return maxWriteMilliseconds;
    }
    
    public void setMaxWriteMilliseconds(long maxWriteMilliseconds) {
        this.maxWriteMilliseconds = maxWriteMilliseconds;
    }
    
    public List<String> getFatalErrors() {
        return fatalErrors;
    }
    
    public void setFatalErrors(List<String> fatalErrors) {
        this.fatalErrors = fatalErrors;
    }
    
    public String getDateField() {
        return dateField;
    }
    
    public void setDateField(String dateField) {
        this.dateField = dateField;
    }
    
    public String getDateFormat() {
        return dateFormat;
    }
    
    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }
    
    public int getFieldLengthThreshold() {
        return fieldLengthThreshold;
    }
    
    public void setFieldLengthThreshold(int fieldLengthThreshold) {
        this.fieldLengthThreshold = fieldLengthThreshold;
    }
    
    public List<String> getIndexFields() {
        return indexFields;
    }
    
    public void setIndexFields(List<String> indexFields) {
        this.indexFields = indexFields;
    }
    
    public List<String> getAdditionalIndexFields() {
        return additionalIndexFields;
    }
    
    public void setAdditionalIndexFields(List<String> additionalIndexFields) {
        this.additionalIndexFields = additionalIndexFields;
    }
    
    public List<String> getReverseIndexFields() {
        return reverseIndexFields;
    }
    
    public void setReverseIndexFields(List<String> reverseIndexFields) {
        this.reverseIndexFields = reverseIndexFields;
    }
    
    public List<String> getAdditionalReverseIndexFields() {
        return additionalReverseIndexFields;
    }
    
    public void setAdditionalReverseIndexFields(List<String> additionalReverseIndexFields) {
        this.additionalReverseIndexFields = additionalReverseIndexFields;
    }
    
    public List<String> getNumericFields() {
        return numericFields;
    }
    
    public void setNumericFields(List<String> numericFields) {
        this.numericFields = numericFields;
    }
    
    public List<String> getAdditionalNumericFields() {
        return additionalNumericFields;
    }
    
    public void setAdditionalNumericFields(List<String> additionalNumericFields) {
        this.additionalNumericFields = additionalNumericFields;
    }
    
    public boolean isEnableBloomFilter() {
        return enableBloomFilter;
    }
    
    public void setEnableBloomFilter(boolean enableBloomFilter) {
        this.enableBloomFilter = enableBloomFilter;
    }
    
    public int getRecordWriterMaxMemory() {
        return recordWriterMaxMemory;
    }
    
    public void setRecordWriterMaxMemory(int recordWriterMaxMemory) {
        this.recordWriterMaxMemory = recordWriterMaxMemory;
    }
    
    public int getRecordWriterMaxLatency() {
        return recordWriterMaxLatency;
    }
    
    public void setRecordWriterMaxLatency(int recordWriterMaxLatency) {
        this.recordWriterMaxLatency = recordWriterMaxLatency;
    }
    
    public int getRecordWriterNumThreads() {
        return recordWriterNumThreads;
    }
    
    public void setRecordWriterNumThreads(int recordWriterNumThreads) {
        this.recordWriterNumThreads = recordWriterNumThreads;
    }
    
    public String getPolicyEnforcerClass() {
        return policyEnforcerClass;
    }
    
    public void setPolicyEnforcerClass(String policyEnforcerClass) {
        this.policyEnforcerClass = policyEnforcerClass;
    }
    
    public String getBaseMaps() {
        return baseMaps;
    }
    
    public void setBaseMaps(String baseMaps) {
        this.baseMaps = baseMaps;
    }
}
