package cn.pandadb.grpc;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Iterator;

public class PandaClient {
    private final PandaRpcServiceGrpc.PandaRpcServiceBlockingStub blockingStub;
//    private final PandaRpcServiceGrpc.PandaRpcServiceStub asyncStub;

    public PandaClient(Channel channel){
        blockingStub = PandaRpcServiceGrpc.newBlockingStub(channel);
//        asyncStub = PandaRpcServiceGrpc.newStub(channel);
    }

    public static void main(String[] args) {
//        ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:50010").usePlaintext().build();
        ManagedChannelBuilder<?> o = ManagedChannelBuilder.forTarget("localhost:50010").usePlaintext();
        ManagedChannel build = o.build();
        PandaClient client = new PandaClient(build);
        client.sendCypher();
        build.shutdown();
    }

    public void sendCypher(){
        CypherRequest request = CypherRequest.newBuilder().setCypher("match (n) return n").build();
        Iterator<InternalRecords> result = blockingStub.getInternalResult(request);
        System.out.println(result);
//        while (result.hasNext()){
//            InternalRecords records = result.next();
//            Iterator<Record> recordsIterator = records.getRecordList().iterator();
//            while (recordsIterator.hasNext()){
//                Record record = recordsIterator.next();
//                String recordName = record.getRecordName();
//
//            }
//        }


//        System.out.println(records.getRecordList());
//        Record record = records.getRecordList().iterator().next();
//        System.out.println(record.getRecordName());
//        record.getContainerMap().keySet().forEach(
//                key -> {
//                    Any any = record.getContainerMap().get(key); // get info
//                    try {
//                        Node node = any.unpack(Node.class); // must know which class to unpack
//                    } catch (InvalidProtocolBufferException e) {
//                        e.printStackTrace();
//                    }
//                }
//        );
    }
}
