package datawave.microservice.querymetric.handler;

import datawave.webservice.query.map.QueryGeometry;
import datawave.webservice.query.map.QueryGeometryResponse;

import java.util.stream.Collectors;

public class QueryMetricGeometryResponse extends QueryGeometryResponse {
    
    // @formatter:off
    private static final String JQUERY_INCLUDES =
            "<script type='text/javascript' src='/querymetric/webjars/jquery/jquery.min.js'></script>\n";
    private static final String LEAFLET_INCLUDES =
            "<link rel='stylesheet' type='text/css' href='/querymetric/webjars/leaflet/leaflet.css' />\n" +
                    "<script type='text/javascript' src='/querymetric/webjars/leaflet/leaflet.js'></script>\n";
    private static final String MAP_INCLUDES =
            "<link rel='stylesheet' type='text/css' href='/querymetric/css/queryMap.css' />\n" +
                    "<script type='text/javascript' src='/querymetric/js/queryMap.js'></script>";
    // @formatter:on
    
    public QueryMetricGeometryResponse(String queryId, String basemaps) {
        super(queryId, basemaps);
    }
    
    @Override
    public String getHeadContent() {
        String basemapData = "<script type='text/javascript'>var basemaps = " + basemaps + ";</script>\n";
        String featureData = "<script type='text/javascript'>var features = " + toGeoJsonFeatures() + ";</script>\n";
        return String.join("\n", featureData, JQUERY_INCLUDES, LEAFLET_INCLUDES, basemapData, MAP_INCLUDES);
    }
    
    @Override
    public String getMainContent() {
        return "<div align='center'><div id='map' style='height: calc(100% - 85px); width: 100%; position: fixed; top: 86px; left: 0px;'></div></div>";
    }
    
    private String toGeoJsonFeatures() {
        if (!this.result.isEmpty())
            return "[ " + this.result.stream().map(QueryGeometry::toGeoJsonFeature).collect(Collectors.joining(", ")) + " ]";
        else
            return "undefined";
    }
}
