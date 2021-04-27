package datawave.microservice.querymetric.config;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.microservice.config.accumulo.AccumuloProperties;
import datawave.webservice.common.connection.AccumuloConnectionPoolFactory;
import datawave.webservice.common.connection.WrappedConnector;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * The InMemoryInstance in this class doesn't cache anything and is only used to create
 * a wrapped connector because the ScannerHelper expects a WrappedConnector and
 * logs an exception if it gets an unwrapped connector
 */
public class WrappedAccumuloConnectionPoolFactory extends AccumuloConnectionPoolFactory {
    private Logger log = LoggerFactory.getLogger(getClass());
    private Connector inMemoryConnector;
    private InMemoryInstance inMemoryInstance;
    
    public WrappedAccumuloConnectionPoolFactory(AccumuloProperties accumuloProperties, Instance instance) {
        super(accumuloProperties.getUsername(), accumuloProperties.getPassword(), instance);
        try {
            inMemoryInstance = new InMemoryInstance();
            inMemoryConnector = inMemoryInstance.getConnector("mock", new PasswordToken("mock"));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    @Override
    public PooledObject<Connector> makeObject() throws Exception {
        return new DefaultPooledObject(new WrappedConnector(super.makeObject().getObject(), inMemoryConnector));
    }
}
