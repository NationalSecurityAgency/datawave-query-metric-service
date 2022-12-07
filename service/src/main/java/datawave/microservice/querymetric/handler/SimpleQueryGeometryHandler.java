package datawave.microservice.querymetric.handler;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.GeoFeatureVisitor;
import datawave.query.language.parser.ParseException;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.map.QueryGeometry;
import datawave.webservice.query.map.QueryGeometryResponse;
import org.apache.commons.jexl2.parser.JexlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static datawave.query.QueryParameters.QUERY_SYNTAX;

/**
 * This class is used to extract query geometries from the query metrics in an effort to provide those geometries for subsequent display to the user.
 */
public class SimpleQueryGeometryHandler implements QueryGeometryHandler {
    private static final Logger log = LoggerFactory.getLogger(ShardTableQueryMetricHandler.class);
    
    private static final String LUCENE = "LUCENE";
    private static final String JEXL = "JEXL";
    
    private LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
    
    private String basemaps;
    
    public SimpleQueryGeometryHandler(QueryMetricHandlerProperties queryMetricHandlerProperties) {
        this.basemaps = queryMetricHandlerProperties.getBaseMaps();
    }
    
    @Override
    public QueryGeometryResponse getQueryGeometryResponse(String id, List<? extends BaseQueryMetric> metrics) {
        QueryGeometryResponse response = new QueryMetricGeometryResponse(id, basemaps);
        
        if (metrics != null) {
            Set<QueryGeometry> queryGeometries = new LinkedHashSet<>();
            for (BaseQueryMetric metric : metrics) {
                try {
                    boolean isLuceneQuery = isLuceneQuery(metric.getParameters());
                    String jexlQuery = (isLuceneQuery) ? toJexlQuery(metric.getQuery()) : metric.getQuery();
                    JexlNode queryNode = JexlASTHelper.parseAndFlattenJexlQuery(jexlQuery);
                    queryGeometries.addAll(GeoFeatureVisitor.getGeoFeatures(queryNode, isLuceneQuery));
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    response.addException(new Exception("Unable to parse the geo features"));
                }
            }
            response.setResult(new ArrayList<>(queryGeometries));
        }
        
        return response;
    }
    
    private static boolean isLuceneQuery(Set<QueryImpl.Parameter> parameters) {
        return parameters.stream().anyMatch(p -> p.getParameterName().equals(QUERY_SYNTAX) && p.getParameterValue().equals(LUCENE));
    }
    
    private String toJexlQuery(String query) throws ParseException {
        return toJexlQuery(query, parser);
    }
    
    private static String toJexlQuery(String query, LuceneToJexlQueryParser parser) throws ParseException {
        return parser.parse(query).getOriginalQuery();
    }
    
    public static boolean isGeoQuery(BaseQueryMetric metric) {
        try {
            String jexlQuery = metric.getQuery();
            if (isLuceneQuery(metric.getParameters()))
                jexlQuery = toJexlQuery(jexlQuery, new LuceneToJexlQueryParser());
            
            return !GeoFeatureVisitor.getGeoFeatures(JexlASTHelper.parseAndFlattenJexlQuery(jexlQuery)).isEmpty();
        } catch (Exception e) {
            log.trace("Unable to parse the geo features", e);
        }
        return false;
    }
}
