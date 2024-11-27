package datawave.microservice.querymetric;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlValue;
import javax.xml.bind.annotation.adapters.XmlAdapter;

/**
 * Provides JAX-B marshalling/unmarshalling of {@link Map} of String to Serializable. This allows the marshalled type to be in our own namespace rather than in
 * the "default" one, which when triggered will then assume the "" prefix and push all of our own elements into the "ns2" prefix.
 */
public class StringSerializableMapAdapter extends XmlAdapter<StringSerializableMapAdapter.StringSerializableMap,Map<String,Serializable>> {
    
    @Override
    public Map<String,Serializable> unmarshal(StringSerializableMap v) throws Exception {
        HashMap<String,Serializable> map = new HashMap<>();
        for (StringSerializableMapEntry entry : v.entries) {
            map.put(entry.key, entry.value);
        }
        return map;
    }
    
    @Override
    public StringSerializableMap marshal(Map<String,Serializable> v) throws Exception {
        StringSerializableMap map = new StringSerializableMap();
        for (Map.Entry<String,Serializable> entry : v.entrySet()) {
            map.entries.add(new StringSerializableMapEntry(entry.getKey(), entry.getValue()));
        }
        return map;
    }
    
    public static class StringSerializableMap {
        @XmlElement(name = "entry")
        private List<StringSerializableMapEntry> entries = new ArrayList<>();
    }
    
    public static class StringSerializableMapEntry {
        @XmlAttribute(name = "name")
        private String key;
        @XmlValue
        private Serializable value;
        
        public StringSerializableMapEntry() {}
        
        public StringSerializableMapEntry(String key, Serializable value) {
            this.key = key;
            this.value = value;
        }
    }
}
