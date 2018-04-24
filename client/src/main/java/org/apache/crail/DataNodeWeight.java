package org.apache.crail;

import java.nio.ByteBuffer;

/**
 * Created by atr on 24.04.18.
 */
public class DataNodeWeight {
    public int ip;
    public int port;
    public float weight;
    public static int CSIZE = ( 2 * Integer.BYTES) + Float.BYTES;

    public int write(ByteBuffer buffer){
        buffer.putInt(ip);
        buffer.putInt(port);
        buffer.putFloat(weight);
        return CSIZE;
    }

    public void update(ByteBuffer buffer){
        this.ip = buffer.getInt();
        this.port = buffer.getInt();
        this.weight = buffer.getFloat();
    }
}
