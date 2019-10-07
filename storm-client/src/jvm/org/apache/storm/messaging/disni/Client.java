/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.storm.messaging.disni;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvSge;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import com.ibm.disni.verbs.SVCPostSend;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IConnectionCallback;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.metric.api.IStatefulObject;
import org.slf4j.LoggerFactory;

// if in storm, use the following (since I don't know how to shade)
import org.apache.storm.shade.io.netty.util.HashedWheelTimer;
import org.apache.storm.shade.io.netty.util.Timeout;
import org.apache.storm.shade.io.netty.util.TimerTask;
//import io.netty.util.HashedWheelTimer;
//import io.netty.util.Timeout;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.netty.BackPressureStatus;

/**
 *
 * @author libreliu
 */
public class Client implements IConnection, Runnable, RdmaEndpointFactory<Client.CustomClientEndpoint>, IStatefulObject {

    private RdmaActiveEndpointGroup<Client.CustomClientEndpoint> endpointGroup;

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Client.class);

    protected final String host;
    protected final int port;

    protected final Map<Long, IDInfo> wrIdMap;

    // work requests waiting for completion
    // we have to record those requests whose wc isn't available yet
    // therefore we can deliver in case of reconnect
    protected final Set<Long> wrIdWaitingForCq;

    /**
     * WrID management. - when dispatchCqEvent,
     *
     * TODO: see if multi RDMAEndpoint instance is better? or global unique? but
     * as we share the same nic, different one must have different partition
     * wrIdMin ~ wrIdMax seems good
     */
    protected final long wrIdMin;
    protected final long wrIdMax;

    /**
     * Indicating whether the connection should be closed.
     */
    private final AtomicBoolean closing;

    /**
     * Indicating whether we should do a reconnect.
     */
    private final AtomicBoolean reconnect;

    private final AtomicLong reconnectAttempts;
    private final AtomicLong messagesSent;
    private final AtomicLong messagesLost;

    protected final BlockingQueue<TaskMessage> tskMsgQueue;

    private Thread ClientMain;

    /**
     * A atomic counter used to give the current free id value for Work Request.
     *
     * Will wrap back as it reaches maximum, but this'll not be a big deal I
     * think.
     */
    protected final AtomicLong wrIdCount;

    // TODO: Upgrade to HashedWheelTimer
    protected final HashedWheelTimer ioTimer;

    private Client.CustomClientEndpoint endpoint;

    Client(String host, int port, AtomicBoolean[] remoteBpStatus, long wrIdMin, long wrIdMax) {
        this.host = host;
        this.port = port;
        this.wrIdMin = wrIdMin;
        this.wrIdMax = wrIdMax;

        this.wrIdMap = new ConcurrentHashMap<>();
        this.wrIdCount = new AtomicLong(wrIdMin);

        this.closing = new AtomicBoolean(false);
        this.reconnect = new AtomicBoolean(false);
        this.tskMsgQueue = new LinkedBlockingQueue<TaskMessage>();
        this.wrIdWaitingForCq = new HashSet<>();
        this.ioTimer = new HashedWheelTimer(); // TODO: USE custom config instead of the default slots

        this.reconnectAttempts = new AtomicLong(0);
        this.messagesLost = new AtomicLong(0);
        this.messagesSent = new AtomicLong(0);

        ClientMain = new Thread(this);
        ClientMain.start();
    }

    @Override
    public void run() {

        try {
            // (int timeout, boolean polling, int maxWR, int maxSge, int cqSize)
            endpointGroup = new RdmaActiveEndpointGroup<Client.CustomClientEndpoint>(1000, false, 128, 4, 128);
            endpointGroup.init(this);

            endpoint = endpointGroup.createEndpoint();

            InetAddress ipAddress = InetAddress.getByName(host);
            InetSocketAddress address = new InetSocketAddress(ipAddress, port);
            endpoint.connect(address, 1); // 1 sec timeout
            LOG.info("ennn");
            InetSocketAddress _addr = (InetSocketAddress) endpoint.getDstAddr();
            LOG.info("DiSNI messaging client connected, address {}", _addr.toString());
            Thread.sleep(1000);
            
            // Connected. TODO: send pingpong packet?
            while (!closing.get()) {
                TaskMessage taken = this.tskMsgQueue.poll(1, TimeUnit.SECONDS);

                if (!reconnect.get()) {
                    if (taken != null) {
                        LOG.info("taken one TaskMessage, prepare to send");

                        //TODO: aggregation policy
                        ByteBuffer msgbuf = taken.serializeDirectWithHeadByte((byte) 0);

                        endpoint.setupSendTask(msgbuf, endpoint.allocateWrId()).execute().free();

                        LOG.info("send event committed successfully");
                    }
                } else {
                    this.tskMsgQueue.add(taken);

                    endpoint.close();
                    endpointGroup.close();

                    try {
                        endpointGroup = new RdmaActiveEndpointGroup<Client.CustomClientEndpoint>(1000, false, 128, 4, 128);
                        endpointGroup.init(this);
                        endpoint = endpointGroup.createEndpoint();
                        endpoint.connect(address, 1); // 1 sec timeout

                        _addr = (InetSocketAddress) endpoint.getDstAddr();
                    } catch (Exception ex) {
                        LOG.warn("Retry once - sleep for few seconds.."); // this is usually because too many ROUTES in a short period
                        Thread.sleep(1000);
                        // retry once
                        try {
                            endpoint.close();
                            endpointGroup.close();
                        } catch (Exception exp) {
                            LOG.warn("got {} once", exp.toString());
                        }
                        
                        endpointGroup = new RdmaActiveEndpointGroup<Client.CustomClientEndpoint>(1000, false, 128, 4, 128);
                        endpointGroup.init(this);
                        endpoint = endpointGroup.createEndpoint();
                        endpoint.connect(address, 1); // 1 sec timeout

                        _addr = (InetSocketAddress) endpoint.getDstAddr();
                    }

                    LOG.info("DiSNI messaging client reconnected, address {}", _addr.toString());

                    LOG.info("Adding failed tasks...");
                    Iterator<Long> it = wrIdWaitingForCq.iterator();
                    while (it.hasNext()) {
                        SVCPostSend snd_call = endpoint.setupRecvRetry(it.next());
                        if (snd_call != null) { // Indicating it's worth to retry
                            snd_call.execute().free();
                        }
                    }

                    reconnect.set(false);
                }

            }

        } catch (Exception ex) {
            LOG.error("Error while launching Client by DiSNI: {}", ex.toString());
        }
    }

    @Override
    public CustomClientEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
        // (group, idPriv, serverSide, clientContext)
        return new Client.CustomClientEndpoint(endpointGroup, idPriv, serverSide, this);
    }

    @Override
    public Object getState() {
        LOG.debug("Getting metrics for client connection to {}", host);
        HashMap<String, Object> ret = new HashMap<String, Object>();
        ret.put("reconnects", reconnectAttempts.getAndSet(0));
        ret.put("sent", messagesSent.getAndSet(0));
        ret.put("pending", wrIdWaitingForCq.size());
        ret.put("lostOnSend", messagesLost.getAndSet(0));
        ret.put("dest", host);
//        String src = srcAddressName();
//        if (src != null) {
//            ret.put("src", src);
//        }
        return ret;
    }

    // Used in WorkerState.java:457
    @Override
    public Map<Integer, Load> getLoad(Collection<Integer> tasks) {
        return new HashMap<Integer, Load>();
    }

    public class CustomClientEndpoint extends RdmaActiveEndpoint {

        private final Client clientContext;

        public CustomClientEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv, boolean serverSide, Client clientContext) throws IOException {
            super(group, idPriv, serverSide);

            this.clientContext = clientContext;
        }

        public long allocateWrId() {
            return clientContext.wrIdCount.getAndIncrement();
        }

        private SVCPostSend setupSendTask(ByteBuffer sendBuf, long wrid) throws IOException {

            ArrayList<IbvSendWR> sendWRs = new ArrayList<IbvSendWR>(1);
            LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();

            IbvMr mr = registerMemory(sendBuf).execute().free().getMr();

            IbvSge sge = new IbvSge();
            sge.setAddr(mr.getAddr());
            sge.setLength(mr.getLength());
            int lkey = mr.getLkey();
            sge.setLkey(lkey);
            sgeList.add(sge);

            IbvSendWR sendWR = new IbvSendWR();
            sendWR.setSg_list(sgeList);
            sendWR.setWr_id(wrid);
            sendWRs.add(sendWR);
            sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
            sendWR.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_SEND.ordinal());

            LOG.info("wrIdMap add wrid={}, IDInfo sge={} mr={}",
                    wrid,
                    mr.toString());

            clientContext.wrIdMap.put(wrid, new IDInfo(sge, mr, sendBuf));

            clientContext.wrIdWaitingForCq.add(wrid);

            return postSend(sendWRs);
        }

        private boolean needRetry(int status) throws IOException {
            switch (status) {
                case 0: //IBV_WC_SUCCESS
                    return false;

                case 13://IBV_WC_RNR_RETRY_EXC_ERR
                case 1: //IBV_WC_LOC_LEN_ERR
                case 2: //IBV_WC_LOC_QP_OP_ERR
                case 3: //IBV_WC_LOC_EEC_OP_ERR
                case 4: //IBV_WC_LOC_PROT_ERR
                case 5: //IBV_WC_WR_FLUSH_ERR
                case 6: //IBV_WC_MW_BIND_ERR
                case 7: //IBV_WC_BAD_RESP_ERR
                case 8: //IBV_WC_LOC_ACCESS_ERR
                case 9: //IBV_WC_REM_INV_REQ_ERR
                case 10://IBV_WC_REM_ACCESS_ERR
                case 11://IBV_WC_REM_OP_ERR
                case 12://IBV_WC_RETRY_EXC_ERR
                case 14://IBV_WC_LOC_RDD_VIOL_ERR
                case 15://IBV_WC_REM_INV_RD_REQ_ERR
                case 16://IBV_WC_REM_ABORT_ERR
                case 17://IBV_WC_INV_EECN_ERR
                case 18://IBV_WC_INV_EEC_STATE_ERR
                case 19://IBV_WC_FATAL_ERR
                case 20://IBV_WC_RESP_TIMEOUT_ERR
                case 21://IBV_WC_GENERAL_ERR
                    return false; // Won't do anything else, just skip this
                default: // ** Will not happen **
                    throw new IOException("Have met unknown ibwc status while deciding whether to retry");
            }
        }

        public SVCPostSend setupRecvRetry(long wrId) throws IOException {

            IDInfo info = clientContext.wrIdMap.get(wrId);

            if (info.retryCount >= 5) { // TODO FIX THIS HARDCODED NUMBER
                LOG.warn("Message (wrid={}) retried {} times, abort",
                        wrId, info.retryCount);
                return null;
            }

            ArrayList<IbvSendWR> sendWRs = new ArrayList<IbvSendWR>(1);
            LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();

            sgeList.add(info.bufSge);

            IbvSendWR sendWR = new IbvSendWR();
            sendWR.setSg_list(sgeList);
            sendWR.setWr_id(wrId);
            sendWRs.add(sendWR);
            sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
            sendWR.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_SEND.ordinal());

            LOG.info("Reattempting to send (retry count {}), wrid={}, IDInfo sge={} mr={}",
                    info.retryCount,
                    wrId,
                    info.bufMr.toString());

            clientContext.wrIdMap.remove(wrId);
            info.retryCount++;
            clientContext.wrIdMap.put(wrId, info);

            clientContext.wrIdWaitingForCq.add(wrId);

            return postSend(sendWRs);
        }

        @Override
        public void dispatchCqEvent(IbvWC wc) throws IOException {
            // Normally this indicates one send is completed successfully.
            // But in some cases this can also indicate
            int status = wc.getStatus();
            long wrId = wc.getWr_id();
            clientContext.wrIdWaitingForCq.remove(wrId);
            if (status == 0) { // Success
                LOG.info("Transferring of message (wrid={}) complete.",
                        wrId);
                // Free the resources
                IDInfo info = clientContext.wrIdMap.get(wrId);
                deregisterMemory(info.bufMr);
                clientContext.wrIdMap.remove(wrId);

            } else if (needRetry(status)) {   // Schedule for retry
                LOG.info("Transferring of message (wrid={}, status={}) failed. scheduling for retry",
                        wrId, status);

                clientContext.ioTimer.newTimeout((Timeout tmt) -> {
                    SVCPostSend snd_call = setupRecvRetry(wrId);
                    if (snd_call != null) { // Indicating it's worth to retry
                        snd_call.execute().free();
                    }
                }, 1, TimeUnit.SECONDS); // TODO: Give variable timeout
                //} else if (status == 13) { // In this case, destroy the existing QP and create a new one is enough --- hard, because QP conn. is established by rdma_connect
                // this is roughly equivalent to reconnect..

            } else {  // Schedule for reconnect
                // Tell us this bad news
                LOG.warn("Got wc status {} (wrid={}), attempting to reconnect",
                        status, wrId);

                // Need to deregister(?) everything, restart, and put things in queue
                IDInfo info = clientContext.wrIdMap.get(wrId);
                deregisterMemory(info.bufMr);

                // Construct new TaskMessage instance
                TaskMessage tskmsg = new TaskMessage(1, null);
                info.bufRef.position(0);
                tskmsg.deserializeWithHeadByte(info.bufRef);

                clientContext.tskMsgQueue.add(tskmsg);
                clientContext.wrIdMap.remove(wrId);

                clientContext.reconnect.set(true);

            }
        }

    }

    @Override
    public void registerRecv(IConnectionCallback cb) {
        throw new UnsupportedOperationException("Client connection should not receive any messages");
    }

    @Override
    public void registerNewConnectionResponse(Supplier<Object> cb) {
        throw new UnsupportedOperationException("Client does not accept new connections");
    }

    @Override
    public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {
        throw new RuntimeException("Client connection should not send load metrics");
    }

    //@Override
    public void sendBackPressureStatus(BackPressureStatus bpStatus) {
        throw new RuntimeException("Client connection should not send BackPressure status");
    }

    // Question: when will msgs modified/deleted? can we just return and put it here?
    @Override
    public void send(Iterator<TaskMessage> msgs) {
        // this method runs in main thread;
        // it just forwards msgs in queue.
        while (msgs.hasNext()) {
            this.tskMsgQueue.add(msgs.next());
            LOG.info("added one message to queue");
        }

    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public void close() {
        try {
            endpoint.close();
            endpointGroup.close();
        } catch (Exception ex) {
            LOG.error(ex.toString());
        }
    }

}
