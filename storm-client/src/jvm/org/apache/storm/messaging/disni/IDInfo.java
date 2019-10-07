/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.storm.messaging.disni;

import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvSge;
import java.nio.ByteBuffer;

/**
 *
 * @author libreliu
 */
public class IDInfo {
    
    public IbvSge bufSge;
    public IbvMr bufMr;
    public ByteBuffer bufRef;
    public int retryCount;
    public int poolId; // ID in MemPool. -1 if not in pool (TODO)
    
    IDInfo(IbvSge bufSge, IbvMr bufMr, ByteBuffer bufRef, int poolId) {
        this.bufSge = bufSge;
        this.bufMr = bufMr;
        this.bufRef = bufRef;
        this.retryCount = 0;
        this.poolId = poolId;
    }
    
    IDInfo(IbvSge bufSge, IbvMr bufMr, ByteBuffer bufRef) {
        this.bufSge = bufSge;
        this.bufMr = bufMr;
        this.bufRef = bufRef;
        this.retryCount = 0;
        this.poolId = -1;
    }
    
}

//       struct ibv_wc {
//               uint64_t                wr_id;          /* ID of the completed Work Request (WR) */
//               enum ibv_wc_status      status;         /* Status of the operation */
//               enum ibv_wc_opcode      opcode;         /* Operation type specified in the completed WR */
//               uint32_t                vendor_err;     /* Vendor error syndrome */
//               uint32_t                byte_len;       /* Number of bytes transferred */
//               union {
//                       __be32                  imm_data;         /* Immediate data (in network byte order) */
//                       uint32_t                invalidated_rkey; /* Local RKey that was invalidated */
//               };
//               uint32_t                qp_num;         /* Local QP number of completed WR */
//               uint32_t                src_qp;         /* Source QP number (remote QP number) of completed WR (valid only for UD QPs) */
//               int                     wc_flags;       /* Flags of the completed WR */
//               uint16_t                pkey_index;     /* P_Key index (valid only for GSI QPs) */
//               uint16_t                slid;           /* Source LID */
//               uint8_t                 sl;             /* Service Level */
//               uint8_t                 dlid_path_bits; /* DLID path bits (not applicable for multicast messages) */
//       };
