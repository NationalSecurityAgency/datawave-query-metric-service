package datawave.microservice.querymetric;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.springframework.web.servlet.ModelAndView;

import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;

@XmlRootElement(name = "QueryMetricListResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryMetricListResponse extends BaseQueryMetricListResponse<QueryMetric> {
    
    private static final long serialVersionUID = 1L;
    private NumberFormat nf = NumberFormat.getIntegerInstance();
    
    /**
     * Creates the ModelAndView for the detailed query metrics page (querymetric.html)
     *
     * @return the ModelAndView for querymetric.html
     */
    @Override
    public ModelAndView createModelAndView() {
        ModelAndView mav = new ModelAndView();
        
        mav.setViewName(viewName);
        
        TreeMap<Date,QueryMetric> metricMap = new TreeMap<>(Collections.reverseOrder());
        
        for (QueryMetric metric : this.getResult()) {
            metricMap.put(metric.getCreateDate(), metric);
        }
        
        List<QueryMetricModel> metricList = new ArrayList<>();
        
        for (QueryMetric metric : metricMap.values()) {
            QueryMetricModel metricModel = new QueryMetricModel(metric, basePath);
            for (PageMetric p : metric.getPageTimes()) {
                metricModel.totalPageTime += p.getReturnTime();
                metricModel.totalPageCallTime += (p.getCallTime()) == -1 ? 0 : p.getCallTime();
                metricModel.totalSerializationTime += (p.getSerializationTime()) == -1 ? 0 : p.getSerializationTime();
                metricModel.totalBytesSent += (p.getBytesWritten()) == -1 ? 0 : p.getBytesWritten();
            }
            metricList.add(metricModel);
        }
        
        mav.addObject("screenCssUrl", basePath + "/css/screen.css");
        mav.addObject("themeCssUrl", basePath + "/css/theme.css");
        if (isGeoQuery) {
            mav.addObject("jqueryMinJsUrl", basePath + "/webjars/jquery/jquery.min.js");
        } else {
            mav.addObject("jqueryMinJsUrl", "");
        }
        mav.addObject("basePath", this.basePath);
        mav.addObject("isGeoQuery", this.isGeoQuery());
        mav.addObject("metricList", metricList);
        return mav;
    }
}
