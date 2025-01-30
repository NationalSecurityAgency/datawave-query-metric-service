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
 * Provides JAX-B marshalling/unmarshalling of {@link Map} of String to List of Integer. This allows the marshalled type to be in our own namespace rather than
 * in the "default" one, which when triggered will then assume the "" prefix and push all of our own elements into the "ns2" prefix.
 */
public class StringIntegerListMapAdapter extends XmlAdapter<StringIntegerListMapAdapter.StringIntegerListMap,Map<String,List<Integer>>> {
    
    @Override
    public Map<String,List<Integer>> unmarshal(StringIntegerListMap v) throws Exception {
        HashMap<String,List<Integer>> map = new HashMap<>();
        for (StringIntegerListMapEntry entry : v.entries) {
            map.put(entry.key, entry.value);
        }
        return map;
    }
    
    @Override
    public StringIntegerListMap marshal(Map<String,List<Integer>> v) throws Exception {
        StringIntegerListMap map = new StringIntegerListMap();
        for (Map.Entry<String,List<Integer>> entry : v.entrySet()) {
            map.entries.add(new StringIntegerListMapEntry(entry.getKey(), entry.getValue()));
        }
        return map;
    }
    
    public static class StringIntegerListMap {
        @XmlElement(name = "entry")
        private List<StringIntegerListMapEntry> entries = new ArrayList<>();
    }
    
    public static class StringIntegerListMapEntry {
        @XmlAttribute(name = "name")
        private String key;
        @XmlValue
        private List<Integer> value;
        
        public StringIntegerListMapEntry() {}
        
        public StringIntegerListMapEntry(String key, List<Integer> value) {
            this.key = key;
            this.value = value;
        }
    }
}
