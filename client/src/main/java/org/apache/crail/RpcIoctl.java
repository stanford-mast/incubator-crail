package org.apache.crail;

/**
 * Created by atr on 11.04.18.
 */

import org.apache.crail.rpc.RpcResponse;

public interface RpcIoctl extends RpcResponse {
    public IOCtlResponse getResponse();
    public void setResponse(IOCtlResponse resp);
}
