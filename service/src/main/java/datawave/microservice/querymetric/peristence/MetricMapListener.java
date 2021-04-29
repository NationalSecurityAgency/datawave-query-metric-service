package datawave.microservice.querymetric.peristence;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.map.listener.EntryLoadedListener;
import com.hazelcast.map.listener.EntryMergedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapEvictedListener;
import datawave.microservice.querymetric.BaseQueryMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/*
 * @see MapClearedListener
 * @see MapEvictedListener
 * @see EntryAddedListener
 * @see EntryEvictedListener
 * @see EntryRemovedListener
 * @see EntryMergedListener
 * @see EntryUpdatedListener
 * @see EntryLoadedListener
 */

public class MetricMapListener implements EntryAddedListener, EntryUpdatedListener, EntryLoadedListener, MapEvictedListener, EntryEvictedListener,
                EntryRemovedListener, EntryMergedListener {
    
    private Logger log = LoggerFactory.getLogger(MetricMapListener.class);
    private String mapName;
    
    public MetricMapListener(String mapName) {
        this.mapName = mapName;
    }
    
    private Long getLastPageNumber(BaseQueryMetric m) {
        Long lastPage = null;
        List<BaseQueryMetric.PageMetric> pageMetrics = m.getPageTimes();
        for (BaseQueryMetric.PageMetric pm : pageMetrics) {
            if (lastPage == null || pm.getPageNumber() > lastPage) {
                lastPage = pm.getPageNumber();
            }
        }
        return lastPage;
    }
    
    private String printEvent(EntryEvent event) {
        Object o = event.getValue();
        if (o instanceof BaseQueryMetric) {
            BaseQueryMetric m = (BaseQueryMetric) o;
            return event.getEventType() + " queryId:" + (m.getQueryId() + " numPages:" + m.getPageTimes().size() + " lastPage:" + getLastPageNumber(m));
        } else {
            return event.toString();
        }
        
    }
    
    @Override
    public void entryAdded(EntryEvent event) {
        log.trace(mapName + " " + printEvent(event));
    }
    
    @Override
    public void entryUpdated(EntryEvent event) {
        log.trace(mapName + " " + printEvent(event));
    }
    
    @Override
    public void entryLoaded(EntryEvent event) {
        log.trace(mapName + " " + printEvent(event));
    }
    
    @Override
    public void entryEvicted(EntryEvent event) {
        log.trace(mapName + " " + printEvent(event));
    }
    
    @Override
    public void entryMerged(EntryEvent event) {
        log.trace(mapName + " " + printEvent(event));
    }
    
    @Override
    public void entryRemoved(EntryEvent event) {
        log.trace(mapName + " " + printEvent(event));
    }
    
    @Override
    public void mapEvicted(MapEvent event) {
        log.trace(mapName + " : " + event.toString());
    }
}
