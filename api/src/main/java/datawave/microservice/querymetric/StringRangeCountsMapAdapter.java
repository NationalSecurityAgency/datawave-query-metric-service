package datawave.microservice.querymetric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlValue;
import javax.xml.bind.annotation.adapters.XmlAdapter;

/**
 * Provides JAX-B marshalling/unmarshalling of {@link Map} of String to RangeCounts. This allows the marshalled type to be in our own namespace rather than in
 * the "default" one, which when triggered will then assume the "" prefix and push all of our own elements into the "ns2" prefix.
 */
public class StringRangeCountsMapAdapter extends XmlAdapter<StringRangeCountsMapAdapter.StringRangeCountsMap,Map<String,RangeCounts>> {
    
    @Override
    public Map<String,RangeCounts> unmarshal(StringRangeCountsMap v) throws Exception {
        HashMap<String,RangeCounts> map = new HashMap<>();
        for (StringRangeCountsMapEntry entry : v.entries) {
            map.put(entry.key, new RangeCounts(entry.documentRangeCount, entry.shardRangeCount));
        }
        return map;
    }
    
    @Override
    public StringRangeCountsMap marshal(Map<String,RangeCounts> v) throws Exception {
        StringRangeCountsMap map = new StringRangeCountsMap();
        for (Map.Entry<String,RangeCounts> entry : v.entrySet()) {
            map.entries.add(new StringRangeCountsMapEntry(entry.getKey(), entry.getValue()));
        }
        return map;
    }
    
    public static class StringRangeCountsMap {
        @XmlElement(name = "entry")
        private List<StringRangeCountsMapAdapter.StringRangeCountsMapEntry> entries = new ArrayList<>();
    }
    
    public static class StringRangeCountsMapEntry {
        @XmlAttribute(name = "name")
        private String key;
        @XmlAttribute
        private long documentRangeCount;
        @XmlAttribute
        private long shardRangeCount;
        
        public StringRangeCountsMapEntry() {}
        
        public StringRangeCountsMapEntry(String key, RangeCounts value) {
            this.key = key;
            this.documentRangeCount = value.getDocumentRangeCount();
            this.shardRangeCount = value.getShardRangeCount();
        }
    }
}
