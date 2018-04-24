package org.apache.crail;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by atr on 24.04.18.
 */
public class WeightMask {
    private ArrayList<DataNodeWeight> mask;

    public WeightMask(){
        this.mask = new ArrayList<>();
    }

    public void addMask(DataNodeWeight w){
        this.mask.add(w);
    }

    public int write(ByteBuffer buffer) throws Exception {
        int items =this.mask.size();
        int size = Integer.BYTES + (items * DataNodeWeight.CSIZE);
        if(buffer.remaining() < size){
            throw new Exception("Size too small");
        }
        buffer.putInt(items);
        for(int i = 0; i < items; i++) {
            this.mask.get(i).write(buffer);
        }
        return size;
    }

    public void update(ByteBuffer buffer) throws Exception {
        int items = buffer.getInt();
        for(int i = 0; i < items; i++) {
            DataNodeWeight w = new DataNodeWeight();
            w.update(buffer);
            this.mask.add(w);
        }
    }
}
