package datawave.microservice.querymetric.config;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.webservice.common.connection.AccumuloConnectionPoolFactory;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class InMemoryAccumuloConnectionPoolFactory extends AccumuloConnectionPoolFactory {

    private InMemoryInstance instance;
    private String userName;
    private String userPassword;

    public InMemoryAccumuloConnectionPoolFactory(String userName, String userPassword, Instance instance) {
        super(userName, userPassword, instance);
        this.userName = userName;
        this.userPassword = userPassword;
        this.instance = (InMemoryInstance)instance;
    }

    @Override public PooledObject<Connector> makeObject() throws Exception {
        Connector c = instance.getConnector(this.userName, new PasswordToken(this.userPassword));
        return new DefaultPooledObject<>(c);
    }
}
