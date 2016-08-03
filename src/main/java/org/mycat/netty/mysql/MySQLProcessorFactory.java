package org.mycat.netty.mysql;

import org.mycat.netty.ProcessorFactory;
import org.mycat.netty.ProtocolProcessor;
import org.mycat.netty.ProtocolTransport;

public class MySQLProcessorFactory implements ProcessorFactory {
    
    private ProtocolProcessor protocolProcessor;
    
    public MySQLProcessorFactory() {
        protocolProcessor = new MySQLProtocolProcessor();
    }

    public ProtocolProcessor getProcessor(ProtocolTransport trans) {
        return protocolProcessor;
    }

}
