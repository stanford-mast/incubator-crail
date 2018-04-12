package org.apache.crail;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by atr on 11.04.18.
 */
abstract public class IOCtlResponse {
    public abstract int write(ByteBuffer buffer) throws IOException;
    public abstract void update(ByteBuffer buffer) throws IOException;
    public abstract int getSize();

    public static class IOCtlNopResp extends IOCtlResponse {
        public static int CSIZE = 0;

        public IOCtlNopResp(){
        }

        public int write(ByteBuffer buffer) throws IOException {
            return IOCtlNopResp.CSIZE;
        }

        public void update(ByteBuffer buffer) throws IOException {
        }

        public int getSize(){
            return IOCtlNopResp.CSIZE;
        }

        public String toString(){
            return "IOCtlNopResp: Empty";
        }
    }

    public static class IOCtlDataNodeRemoveResp extends IOCtlResponse {
        public static int CSIZE = 0;

        public IOCtlDataNodeRemoveResp(){
        }

        public int write(ByteBuffer buffer) throws IOException {
            return IOCtlDataNodeRemoveResp.CSIZE;
        }

        public void update(ByteBuffer buffer) throws IOException {
        }

        public int getSize(){
            return IOCtlDataNodeRemoveResp.CSIZE;
        }

        public String toString(){
            return "IOCtlResponse: Empty";
        }
    }

    public static class GetClassStatResp extends IOCtlResponse {
        public static int CSIZE = 16;
        private long allBlocks;
        private long freeBlocks;

        public GetClassStatResp(){
            this.allBlocks = -1;
            this.freeBlocks = -1;
        }

        public GetClassStatResp(long all, long consumed){
            this.allBlocks = all;
            this.freeBlocks = consumed;
        }

        public int write(ByteBuffer buffer) throws IOException {
            if(GetClassStatResp.CSIZE > buffer.remaining()) {
                throw new IOException("Write ByteBuffer is too small, remaining " + buffer.remaining() + " expected, " + GetClassStatResp.CSIZE + " bytes");
            }
            // write 2 longs
            buffer.putLong(this.allBlocks);
            buffer.putLong(this.freeBlocks);
            return GetClassStatResp.CSIZE;
        }

        public void update(ByteBuffer buffer) throws IOException {
            if(getSize() > buffer.remaining()) {
                throw new IOException("Read ByteBuffer is too small, remaining " + buffer.remaining() + " expected, " + getSize() + " bytes");
            }
            this.allBlocks = buffer.getLong();
            this.freeBlocks = buffer.getLong();
        }

        public int getSize(){
            return GetClassStatResp.CSIZE;
        }

        public String toString(){
            return "GetClassStatResp: all block: " + this.allBlocks + " free: " + this.freeBlocks;
        }
    }
}
