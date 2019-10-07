/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.storm.messaging.disni;

import com.ibm.disni.verbs.IbvSge;
import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 *
 * @author libreliu
 */
public class MemDesc {
    
    public int id;
    public LinkedList<IbvSge> sgeList;
    public ByteBuffer buf;
    
    MemDesc(int id, LinkedList<IbvSge> sgeList, ByteBuffer buf) {
        this.id = id;
        this.sgeList = sgeList;
        this.buf = buf;
    }
}
