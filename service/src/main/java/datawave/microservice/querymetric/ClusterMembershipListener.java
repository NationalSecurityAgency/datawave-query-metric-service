package datawave.microservice.querymetric;

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import org.apache.log4j.Logger;

import java.util.Set;

public class ClusterMembershipListener implements MembershipListener {
    
    private Logger log = Logger.getLogger(ClusterMembershipListener.class);
    
    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        Set<Member> members = membershipEvent.getCluster().getMembers();
        log.info("added member: " + membershipEvent.getMember().getUuid() + ":" + membershipEvent.getMember().getAddress().toString() + " cluster now ("
                        + members.size() + ") " + members);
    }
    
    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        Set<Member> members = membershipEvent.getCluster().getMembers();
        log.info("removed member: " + membershipEvent.getMember().getUuid() + ":" + membershipEvent.getMember().getAddress().toString() + " cluster now ("
                        + members.size() + ") " + members);
    }
    
    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        
    }
}
