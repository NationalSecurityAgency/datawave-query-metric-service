package datawave.microservice.querymetric;

import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import org.apache.log4j.Logger;

public class ClusterMembershipListener implements MembershipListener {
    
    private Logger log = Logger.getLogger(ClusterMembershipListener.class);
    
    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        log.info("member added: " + membershipEvent.getMember().getUuid() + ":" + membershipEvent.getMember().getAddress().toString());
    }
    
    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        log.info("member removed: " + membershipEvent.getMember().getUuid() + ":" + membershipEvent.getMember().getAddress().toString());
    }
    
    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        
    }
}
