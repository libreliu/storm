/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.storm.messaging.disni;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.storm.messaging.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author libreliu
 */
public class Context implements IContext {
    
    private Map<String, Object> topoConf;
    
    private static final Logger LOG = LoggerFactory.getLogger(Context.class);
    
    private Server commServer;
    
    /**
     * initialization per Storm configuration
     */
    @Override
    public void prepare(Map<String, Object> topoConf) {
        this.topoConf = topoConf;
        
        commServer = null;
    }
    
    /**
     * establish a server with a binding port
     */
    @Override
    public synchronized IConnection bind(String storm_id, int port) {
        LOG.info("calling bind with parameter storm_id={}, port={}", storm_id, port);
        if (commServer != null) {
            LOG.error("already bond to somewhere. abort.");
            throw new RuntimeException("already bond to somewhere. abort."); //To change body of generated methods, choose Tools | Templates.
        }
        commServer = new Server(port, topoConf, 0, 10000);
        
        return commServer;
    }
    
    /**
     * establish a connection to a remote server
     */
    @Override
    public IConnection connect(String storm_id, String host, int port, AtomicBoolean[] remoteBpStatus) {
        return new Client(host, port, remoteBpStatus, 10000, 20000);
    }
    
    /**
     * terminate this context
     */
    @Override
    public synchronized void term() {
        
        
    }
}
