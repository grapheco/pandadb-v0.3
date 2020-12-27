package cn.pandadb.driver;

import cn.pandadb.grpc.CypherRequest;
import cn.pandadb.grpc.InternalRecords;
import cn.pandadb.grpc.PandaRpcServiceGrpc;
import cn.pandadb.grpc.Record;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Iterator;

public class PandaRpcClient {
    ManagedChannel channel;
    private final PandaRpcServiceGrpc.PandaRpcServiceBlockingStub blockingStub;
//    private final PandaRpcServiceGrpc.PandaRpcServiceStub asyncStub;

    public PandaRpcClient(ManagedChannel channel){
        this.channel = channel;
        blockingStub = PandaRpcServiceGrpc.newBlockingStub(channel);
//        asyncStub = PandaRpcServiceGrpc.newStub(channel);
    }

//    public static void main(String[] args) {
//        ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:50010").usePlaintext().build();
//        PandaRpcClient client = new PandaRpcClient(channel);
//        client.sendCypher();
//        channel.shutdown();
//    }

    public String sendCypher(){
        CypherRequest request = CypherRequest.newBuilder().setCypher("match (n) return n").build();
        Iterator<InternalRecords> result = blockingStub.getInternalResult(request);
        return result.next().getRecordList().get(0).getRecordName();
    }
    public void close(){
       channel.shutdown();
    }
}
