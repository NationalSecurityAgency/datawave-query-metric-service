package datawave.microservice.querymetric;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import com.fasterxml.jackson.annotation.JsonIgnore;

import datawave.webservice.HtmlProvider;
import datawave.webservice.result.BaseResponse;

/**
 * This response includes information about what query geometries were present in a given query. The geometries are displayed on a map using leaflet.
 */
@XmlRootElement(name = "QueryGeometry")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryGeometryResponse extends BaseResponse implements HtmlProvider {
    
    private static final long serialVersionUID = 1L;
    
    private static final String TITLE = "Query Geometry";
    
    private String LEAFLET_INCLUDES;
    private String JQUERY_INCLUDES;
    private String MAP_INCLUDES;
    
    public QueryGeometryResponse() {
        this(null, null);
    }
    
    public QueryGeometryResponse(String queryId, String basemaps) {
        this.queryId = queryId;
        this.basemaps = basemaps;
        setHtmlIncludePaths(new HashMap<>());
    }
    
    public void setHtmlIncludePaths(Map<String,String> pathMap) {
        // @formatter:off
        LEAFLET_INCLUDES =
                "<link rel='stylesheet' type='text/css' href='" + pathMap.getOrDefault("leaflet", "") + "/leaflet.css' />\n" +
                        "<script type='text/javascript' src='" + pathMap.getOrDefault("leaflet", "") + "/leaflet.js'></script>\n";
        JQUERY_INCLUDES =
                "<script type='text/javascript' src='" + pathMap.getOrDefault("jquery", "") + "/jquery.min.js'></script>\n";
        MAP_INCLUDES =
                "<link rel='stylesheet' type='text/css' href='" + pathMap.getOrDefault("css", "") + "/queryMap.css' />\n" +
                        "<script type='text/javascript' src='" + pathMap.getOrDefault("js", "") + "/queryMap.js'></script>";
        // @formatter:on
    }
    
    @XmlElement(name = "queryId", nillable = true)
    protected String queryId = null;
    
    @JsonIgnore
    @XmlTransient
    protected String basemaps = null;
    
    @XmlElementWrapper(name = "features")
    @XmlElement(name = "feature")
    protected List<QueryGeometry> result = null;
    
    @JsonIgnore
    @XmlTransient
    @Override
    public String getTitle() {
        if (queryId != null)
            return TITLE + " - " + queryId;
        return TITLE;
    }
    
    @JsonIgnore
    @XmlTransient
    @Override
    public String getHeadContent() {
        String basemapData = "<script type='text/javascript'>var basemaps = " + basemaps + ";</script>\n";
        String featureData = "<script type='text/javascript'>var features = " + toGeoJsonFeatures() + ";</script>\n";
        return String.join("\n", featureData, JQUERY_INCLUDES, LEAFLET_INCLUDES, basemapData, MAP_INCLUDES);
    }
    
    @JsonIgnore
    @XmlTransient
    @Override
    public String getPageHeader() {
        return getTitle();
    }
    
    @JsonIgnore
    @XmlTransient
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
    
    public String getQueryId() {
        return queryId;
    }
    
    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }
    
    public List<QueryGeometry> getResult() {
        return result;
    }
    
    public void setResult(List<QueryGeometry> result) {
        this.result = result;
    }
    
    public String getBasemaps() {
        return basemaps;
    }
    
    public void setBasemaps(String basemaps) {
        this.basemaps = basemaps;
    }
}
