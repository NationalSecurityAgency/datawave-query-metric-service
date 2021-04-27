package datawave.microservice.querymetric.logic;

import datawave.query.tables.ShardQueryLogic;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

public class QueryMetricQueryLogic extends ShardQueryLogic {
    
    private Collection<String> roles = null;
    
    public QueryMetricQueryLogic() {
        super();
    }
    
    public QueryMetricQueryLogic(QueryMetricQueryLogic other) {
        super(other);
        if (other.roles != null) {
            roles = new ArrayList<>();
            roles.addAll(other.roles);
        }
    }
    
    @Override
    public QueryMetricQueryLogic clone() {
        return new QueryMetricQueryLogic(this);
    }
    
    @Override
    public final GenericQueryConfiguration initialize(Connector connection, Query settings, Set<Authorizations> auths) throws Exception {
        return super.initialize(connection, settings, auths);
    }
}
