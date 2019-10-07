/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.storm.messaging.disni;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvRecvWR;
import com.ibm.disni.verbs.IbvSge;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import com.ibm.disni.verbs.SVCPostRecv;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Level;
import org.apache.storm.Config;
import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IConnectionCallback;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.messaging.netty.BackPressureStatus;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The actual Server (run in a separate thread, wrapped by ServerRunner).
 *
 * Reasonably, as a Thread, it needs to communicate with the outer space
 * (Synchronizing RecvCallback() info and other stuff)
 *
 * Note on things to consider: - if a client sends too fast, it'll get Wc with
 * status 13 (indicating no proper send request available)
 *
 * @author libreliu
 */
public class Server implements RdmaEndpointFactory<Server.CustomServerEndpoint>, Runnable, IConnection {

    private final int port;
    private RdmaActiveEndpointGroup<Server.CustomServerEndpoint> endpointGroup;

    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    /**
     * Name of interfaces to filter while binding addresses.
     *
     * This is set to solve binding to the wrong interface & IP address, since
     * binding to 0.0.0.0 simply don't work in DiSNI,
     *
     * See "bindingFilterStrict" for more details on filtering.
     */
    private final List<String> bindingFilterList;
    /**
     * Control how interfaces to be bound to should be filtered.
     *
     * When set to true, only interfaces whose names exactly matched with one of
     * the bindingFilterList will be filtered out.
     *
     * When set to false, all interfaces whose name partly fell in the list will
     * also take into account.
     */
    private final boolean bindingFilterStrict;

    /**
     * Control whether interfaces to be bound to should be filtered.
     *
     * When set to true, binding Filter is enabled.
     *
     * When set to false, binding Filter is disabled.
     */
    private final boolean bindingFilterEnable;

    private Thread ServerMain;

    private LinkedList<Server.CustomServerEndpoint> acceptedEndpoints;

    private AtomicBoolean closing;

    protected IConnectionCallback recvCb;

    /**
     * Concurrent recv initialization tasks.
     */
    protected final int recvCallInitialized;

    /**
     * A atomic counter used to give the current free id value for Work Request.
     *
     * Will wrap back as it reaches maximum, but this'll not be a big deal I
     * think.
     */
    protected AtomicLong wrIdCount;

    // ID -> ID Specific info Buf used (IbvSge Elem)
    // For performance considerations, only one continous chunk could be transmitted
    // at this moment
    protected Map<Long, IDInfo> wrIdMap;

    /**
     * WrID management. - when dispatchCqEvent,
     *
     * TODO: see if multi RDMAEndpoint instance is better? or global unique? but
     * as we share the same nic, different one must have different partition
     * wrIdMin ~ wrIdMax seems good
     */
    protected final long wrIdMin;

    protected final long wrIdMax;

    private final int bufferSize;

    @SuppressWarnings("unchecked")
    Server(int port, Map<String, Object> topoConf, long wrIdMin, long wrIdMax) {
        this.port = port;
        LOG.info("storm-rdma server initialized, port={}", port);

//        this.bindingFilterEnable = true;
//        this.bindingFilterStrict = false;
//        this.bindingFilterList = new ArrayList<>();
//        this.bindingFilterList.add("vir");
//        this.bindingFilterList.add("docker");
//        this.bindingFilterList.add("tun0");
        this.bindingFilterEnable = (Boolean) topoConf.get(Config.STORM_MESSAGING_DISNI_BINDING_FILTER_ENABLE);
        this.bindingFilterStrict = (Boolean) topoConf.get(Config.STORM_MESSAGING_DISNI_BINDING_FILTER_STRICT);
        LOG.info("DiSNI bindingFilterEnable={}, bindingFilterStrict={}", bindingFilterEnable, bindingFilterStrict);

        /* Write like this in yaml:
            storm.zookeeper.servers:
                - "localhost"
           And then it'll be List<String> here.
         */
        this.bindingFilterList = (List<String>) topoConf.get(Config.STORM_MESSAGING_DISNI_BINDING_FILTER_LIST);

        for (String str : bindingFilterList) {
            LOG.info("added {} into the binding filter", str);
        }

        this.bufferSize = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_DISNI_RECV_BUFFER_SIZE));
        LOG.info("DiSNI recv.buffer.size={}", this.bufferSize);

        this.recvCallInitialized = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_DISNI_RECV_CALL_INITIALIZED));
        LOG.info("DiSNI recvCallInitialized={}", this.recvCallInitialized);

        this.acceptedEndpoints = new LinkedList<>();
        this.closing = new AtomicBoolean(false);
        this.recvCb = null;
        this.wrIdMap = new ConcurrentHashMap<>();
        this.wrIdMin = wrIdMin;
        this.wrIdMax = wrIdMax;
        this.wrIdCount = new AtomicLong(wrIdMin);

        ServerMain = new Thread(this);
        ServerMain.start();

    }

    /**
     *
     */
    @Override
    public void run() {

        try {
            // (int timeout, boolean polling, int maxWR, int maxSge, int cqSize)
            endpointGroup = new RdmaActiveEndpointGroup<Server.CustomServerEndpoint>(1000, false, 128, 4, 128);
            endpointGroup.init(this);

            RdmaServerEndpoint<Server.CustomServerEndpoint> serverEndpoint = endpointGroup.createServerEndpoint();

            InetAddress ipAddress = InetAddress.getByName(this.getLocalServerIp());
            InetSocketAddress address = new InetSocketAddress(ipAddress, port);

            // TODO: make backlog adjustable
            serverEndpoint.bind(address, 10);
            LOG.info("Done binding to address {}", address.toString());

            while (!closing.get()) {
                // todo: add timeout, or there'll be problem closing
                Server.CustomServerEndpoint endpoint = serverEndpoint.accept();
                LOG.info("Connection accepted");

                // add to the list
                acceptedEndpoints.add(endpoint);
            }

//            Iterator it = acceptedEndpoints.iterator();
//            while (it.hasNext()) { //TODO: remote close <-> acceptedEndpoints.delete
//                try {
//                    it.next().close();
//                }
//            }
            endpointGroup.close();

        } catch (Exception ex) {
            LOG.error("Error while launching Server by DiSNI: {}", ex.toString());
        }
    }

    /**
     * Get local server ip, for binding usages.
     *
     * See {@link Server#bindingFilterList} and
     * {@link Server#bindingFilterStrict} for more.
     *
     * @return IP Address
     */
    private String getLocalServerIp() {

        try {
            for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {

                NetworkInterface intf = en.nextElement();

                // Test for validity
                if (bindingFilterEnable) {
                    if (this.bindingFilterStrict) {
                        if (this.bindingFilterList.contains(intf.toString())) {
                            LOG.info("Interface {} matches filter list item {}, discard in Strict mode.", intf.toString(), intf.toString());
                            continue;
                        }
                    } else {
                        boolean inList = false;
                        for (String rule : this.bindingFilterList) {
                            if (intf.toString().contains(rule)) {
                                LOG.info("Interface {} matches filter list item {}, discard in Loose mode.", intf.toString(), rule);
                                inList = true;
                                break;
                            }
                        }

                        if (inList) {
                            continue;
                        }
                    }
                }

                for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements();) {
                    InetAddress inetAddress = enumIpAddr.nextElement();

                    if (!inetAddress.isLoopbackAddress() && !inetAddress.isLinkLocalAddress() && inetAddress.isSiteLocalAddress()) {
                        LOG.info("(Chosen) InetAddress {} for Interface {}", inetAddress.getHostAddress().toString(), intf.toString());
                        return inetAddress.getHostAddress().toString();
                    } else {
                        LOG.info("(Discard) InetAddress {} for Interface {}", inetAddress.getHostAddress().toString(), intf.toString());
                    }
                }
            }
        } catch (SocketException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    @Override
    public CustomServerEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        // (group, idPriv, serverSide, bufferSize, serverContext)
        return new Server.CustomServerEndpoint(endpointGroup, id, true, bufferSize, this);
    }

    @Override
    public void sendBackPressureStatus(BackPressureStatus bpStatus) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    // Used in WorkerState.java:457
    @Override
    public Map<Integer, Load> getLoad(Collection<Integer> tasks) {
        throw new RuntimeException("Server connection cannot get load");
    }

    public static class CustomServerEndpoint extends RdmaActiveEndpoint {

//        private ByteBuffer buffers[];
//        private IbvMr mrlist[];
//        private int buffercount = 3;
//
//        private ByteBuffer dataBuf;
//        private IbvMr dataMr;
//        private ByteBuffer sendBuf;
//        private IbvMr sendMr;
//        private ByteBuffer recvBuf;
//        private IbvMr recvMr;
//
//        private LinkedList<IbvSendWR> wrList_send;
//        private IbvSge sgeSend;
//        private LinkedList<IbvSge> sgeList;
//        private IbvSendWR sendWR;
//
//        private LinkedList<IbvRecvWR> wrList_recv;
//        private IbvSge sgeRecv;
//        private LinkedList<IbvSge> sgeListRecv;
//        private IbvRecvWR recvWR;
//
//        private ArrayBlockingQueue<IbvWC> wcEvents;
        private final int bufferSize;

        private LinkedList<ByteBuffer> buffers;

        //private 
        private final Server serverContext;

        private MemPool memPool;

        /**
         * Responsible for one client.
         *
         *
         * @param endpointGroup
         * @param idPriv
         * @param serverSide
         * @param buffersize
         * @param serverContext
         * @throws IOException
         */
        public CustomServerEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> endpointGroup,
                RdmaCmId idPriv,
                boolean serverSide,
                int buffersize,
                Server serverContext) throws IOException {

            super(endpointGroup, idPriv, serverSide);

            this.bufferSize = buffersize;
            this.serverContext = serverContext;
        }

        private SVCPostRecv setupRecvTask(ByteBuffer recvBuf, long wrid) throws IOException {
            ArrayList<IbvRecvWR> recvWRs = new ArrayList<IbvRecvWR>(1);
            LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();

            IbvMr mr = registerMemory(recvBuf).execute().free().getMr();

            IbvSge sge = new IbvSge();
            sge.setAddr(mr.getAddr());
            sge.setLength(mr.getLength());
            int lkey = mr.getLkey();
            sge.setLkey(lkey);
            sgeList.add(sge);

            IbvRecvWR recvWR = new IbvRecvWR();
            recvWR.setWr_id(wrid);
            recvWR.setSg_list(sgeList);
            recvWRs.add(recvWR);

            LOG.info("wrIdMap add wrid={}, IDInfo sge={} mr={} recvbuf={}",
                    wrid,
                    mr.toString(),
                    recvBuf.toString());

            serverContext.wrIdMap.put(wrid, new IDInfo(sge, mr, recvBuf, -1));

            return postRecv(recvWRs);
        }

        // MemPool version
        private SVCPostRecv setupRecvTask(long wrid) throws IOException, MemPool.PoolFullException {
            ArrayList<IbvRecvWR> recvWRs = new ArrayList<IbvRecvWR>(1);

            MemDesc desc = memPool.takeOut();

            IbvRecvWR recvWR = new IbvRecvWR();
            recvWR.setWr_id(wrid);
            recvWR.setSg_list(desc.sgeList);
            recvWRs.add(recvWR);

            LOG.info("wrIdMap add (taken by memPool id={}) wrid={}",
                    desc.id,
                    wrid
            );

            serverContext.wrIdMap.put(wrid, new IDInfo(desc.sgeList.get(0), null, desc.buf, desc.id));

            return postRecv(recvWRs);
        }

        // MemPool version - taken out by user
        private SVCPostRecv setupRecvTask(MemDesc desc, long wrid) throws IOException, MemPool.PoolFullException {
            ArrayList<IbvRecvWR> recvWRs = new ArrayList<IbvRecvWR>(1);

            IbvRecvWR recvWR = new IbvRecvWR();
            recvWR.setWr_id(wrid);
            recvWR.setSg_list(desc.sgeList);
            recvWRs.add(recvWR);

            LOG.info("wrIdMap add (taken by user, mempool id={}) wrid={}",
                    desc.id,
                    wrid
            );

            serverContext.wrIdMap.put(wrid, new IDInfo(desc.sgeList.get(0), null, desc.buf, desc.id));
            return postRecv(recvWRs);
        }

        private long allocateWrId() {
            return serverContext.wrIdCount.getAndIncrement();
        }

        public void init() throws IOException {
            super.init();

            this.memPool = new MemPool(serverContext.recvCallInitialized + 10, bufferSize, this);
            
            // !!This matters!!
            for (int i = 0; i < serverContext.recvCallInitialized; i++) {
                try {
                    setupRecvTask(allocateWrId()).execute().free();
                } catch (MemPool.PoolFullException ex) {
                    throw new IOException(ex);
                }
            }

            LOG.info("SimpleServer::initiated recv");
        }

        private static void dumpByteBuffer(ByteBuffer buf) {
            for (int i = 0; i < buf.limit(); i++) {
                LOG.info("data[{}]={}", i, buf.get(i));
            }
        }

        /**
         * (Guess) Should be called on new Completion events arrive
         *
         * @param wc
         * @throws IOException
         */
        @Override
        public void dispatchCqEvent(IbvWC wc) throws IOException {
            //TODO: Implement Me
            if (serverContext.recvCb != null) {
                // Find where the data stored, then give it away
                IDInfo info = serverContext.wrIdMap.get(wc.getWr_id());

                if (info == null) {
                    throw new IOException("Can't match Work Request ID with previously assigned map.");
                }

                // Byte transferred
                int transferred = wc.getByte_len();

                // Head Pointer
                ByteBuffer data = info.bufRef;

                // Decode it, and use slice technique to avoid copying
                byte first = data.get(0);

                /*         
                 * Brief on protocol design:
                 * 
                 * - First byte == 00: TaskMessage (To be passed to upper layer)
                 * - First byte == 01: Next Message is larger than normal buffer size
                 *   Second 8 byte: Message size
                 * - First byte == 02: Negotiate new buffer size to x byte
                 *   Second 8 byte: Buffer size
                 * - First byte == 03: N/A
                 */
                // Special Notice: Java internally uses UTF-16, so a byte[] constructed 
                // by a String with no explicit encodings are encoded to UTF-16.
                // In UTF-16 everything uses two bytes at least, for ascii the leading byte
                // is always zero.
                // (char[] == UTF-16 Character array, roughly short int [] in C)
                //LOG.info("data: {}", data.asCharBuffer().toString());
                //dumpByteBuffer(data);
                if (first == 0) {
                    LOG.info("first=0, normal TaskMessage object");
                    data.position(1);
                    data.limit(transferred); // new viewport: buf[1...transferred - 1]
                    ByteBuffer tmsgbuf = data.slice();

                    // TODO: let it be rdma!
                    TaskMessage tmsg = new TaskMessage(0, null);
                    tmsg.deserialize(tmsgbuf);

                    // Construct a List with one element
                    LinkedList<TaskMessage> retlist = new LinkedList<>();
                    retlist.add(tmsg);

                    serverContext.recvCb.recv(retlist);

                    if (info.poolId == -1) {
                        // free - p.s. in previous tmsg construct this can be done earlier
                        deregisterMemory(info.bufMr);
                    } else {
                        MemDesc desc = new MemDesc(info.poolId, null, null);
                        memPool.takeIn(desc);
                    }

                    serverContext.wrIdMap.remove(wc.getWr_id());

                    // Setup another recv
                    
                    try {
                        // register (in my own struct) and post the recv.
                        setupRecvTask(allocateWrId()).execute().free();
                    } catch (MemPool.PoolFullException ex) {
                        LOG.error("Pool Full: {}", ex.toString());
                    }

                    LOG.info("finished dispatchCqEvent");
                }
            }
        }
    }

    @Override
    public void registerRecv(IConnectionCallback cb) {
        recvCb = cb;
    }

    // This is used in WorkerState.java:501, to send BackPressure to new clients
    @Override
    public void registerNewConnectionResponse(Supplier<Object> cb) {
        // No-op
    }

    // This is used in WorkerState.java:462, to broadcast Load Metrics
    @Override
    public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {
        // No-op
    }

    @Override
    public void send(Iterator<TaskMessage> msgs) {
        throw new UnsupportedOperationException("Server connection should not send any messages");
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public void close() {
        this.closing.set(true);
        try {
            endpointGroup.close(); // todo fix this
        } catch (Exception ex) {
            LOG.error(ex.toString());
        }
    }
}
