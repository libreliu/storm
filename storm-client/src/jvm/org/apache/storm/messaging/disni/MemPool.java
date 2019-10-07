/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.storm.messaging.disni;

import com.ibm.disni.RdmaEndpoint;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvSge;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

/**
 *
 * @author libreliu
 */
public class MemPool {
    
    private final ByteBuffer[] bufArr;
    private final IbvMr[] mrArr;
    private final ArrayList<LinkedList<IbvSge>> sgeListArr;
    
    private final Set<Integer> freeCell;
    private final Set<Integer> usedCell;
    
    public MemDesc takeOut() throws PoolFullException {
        // find first available candidate
        Iterator<Integer> it = freeCell.iterator();
        if (!it.hasNext()) {
            throw new PoolFullException();
        }
        
        int id = it.next();
        MemDesc desc = new MemDesc(id, sgeListArr.get(id), bufArr[id]);
        
        it.remove();
        usedCell.add(id);
        
        return desc;
    }
    
    public void takeIn(MemDesc desc) {
        freeCell.add(desc.id);
        usedCell.remove(desc.id);
    }

    MemPool(int nmemb, int size, RdmaEndpoint ep) throws IOException {
        bufArr = new ByteBuffer[nmemb];
        mrArr = new IbvMr[nmemb];
        sgeListArr = new ArrayList<LinkedList<IbvSge>>(nmemb);
        freeCell = new HashSet<Integer>();
        usedCell = new HashSet<Integer>();
        
        for (int i = 0; i < nmemb; i++) {
            bufArr[i] = ByteBuffer.allocateDirect(size);
            mrArr[i] = ep.registerMemory(bufArr[i]).execute().free().getMr();
            IbvSge sge = new IbvSge();
            sge.setAddr(mrArr[i].getAddr());
            sge.setLength(mrArr[i].getLength());
            sge.setLkey(mrArr[i].getLkey());
            sgeListArr.add(i, new LinkedList<IbvSge>());
            sgeListArr.get(i).add(sge);
            
            freeCell.add(i);
        }
    }

    public static class PoolFullException extends Exception {

        private static final long serialVersionUID = 1L;

        public PoolFullException() {
            super("Memory Pool has already full.");
        }
    }

}
