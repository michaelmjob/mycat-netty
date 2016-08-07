package io.mycat.netty.mysql;

import io.mycat.netty.ProtocolProcessor;
import io.mycat.netty.ProtocolTransport;
import io.mycat.netty.ProcessorFactory;

public class MySQLProcessorFactory implements ProcessorFactory {
    
    private ProtocolProcessor protocolProcessor;
    
    public MySQLProcessorFactory() {
        protocolProcessor = new MySQLProtocolProcessor();
    }

    public ProtocolProcessor getProcessor(ProtocolTransport trans) {
        return protocolProcessor;
    }

}
