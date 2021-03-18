package datawave.microservice.querymetrics.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import datawave.webservice.query.metric.BaseQueryMetric;

import java.io.IOException;

public class BaseQueryMetricDeserializer extends StdDeserializer<BaseQueryMetric> {
    
    private static final long serialVersionUID = 1L;
    
    private Class<? extends BaseQueryMetric> subClass;
    
    public BaseQueryMetricDeserializer(Class<? extends BaseQueryMetric> subClass) {
        super(BaseQueryMetric.class);
        this.subClass = subClass;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.fasterxml.jackson.databind.JsonDeserializer#deserialize(com.fasterxml.jackson.core.JsonParser,
     * com.fasterxml.jackson.databind.DeserializationContext)
     */
    @Override
    public BaseQueryMetric deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        
        ObjectMapper mapper = (ObjectMapper) jp.getCodec();
        ObjectNode obj = mapper.readTree(jp);
        return mapper.treeToValue(obj, subClass);
    }
}
