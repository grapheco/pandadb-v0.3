//import cn.pandadb.grpc.CypherRequest;
//import cn.pandadb.grpc.InternalRecords;
//import cn.pandadb.grpc.PandaRpcServiceGrpc;
//import cn.pandadb.grpc.Record;
//import cn.pandadb.server.values.Value;
//import com.google.protobuf.Any;
//import io.grpc.Server;
//import io.grpc.ServerBuilder;
//import io.grpc.stub.StreamObserver;
//
//import java.io.IOException;
//import java.util.Iterator;
//
//public class PandaRpcServer {
//    private Server server;
//    GenerateDatabase db;
//
//    public void start(int port) throws IOException {
//        db = new GenerateDatabase();
//        db.init();
//        server = ServerBuilder.forPort(port)
//                .addService(new PandaRpcServer.PandaServiceImpl(db)).build().start();
//
//        System.out.println("started...");
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            @Override
//            public void run() {
//                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
//                System.err.println("*** shutting down pandaRpc server since JVM is shutting down");
//                PandaRpcServer.this.stop();
//                System.err.println("*** server shut down");
//            }
//        });
//    }
//
//    private void stop() {
//        if (server != null) {
//            server.shutdown();
//        }
//    }
//
//    public void blockUntilShutdown() throws InterruptedException {
//        if (server != null) {
//            server.awaitTermination();
//        }
//    }
//
//    public static void main(String[] args) throws IOException, InterruptedException {
//        PandaRpcServer server = new PandaRpcServer();
//        server.start(50010);
//        System.out.println("started...");
//        server.blockUntilShutdown();
//    }
//
//
//
//    private static class PandaServiceImpl extends PandaRpcServiceGrpc.PandaRpcServiceImplBase {
//        GenerateDatabase pdb;
//        public PandaServiceImpl(GenerateDatabase db){
//            pdb = db;
//        }
//        @Override
//        public void getInternalResult(CypherRequest cypherRequest, StreamObserver<InternalRecords> responseObserver) {
//            System.out.println("get...." + cypherRequest.getCypher());
//
//            InternalRecords.Builder internalRecordsBuilder = InternalRecords.newBuilder();
//            // TODO: support stream of InternalRecords
//            Iterator<Value> values = pdb.runCypher(cypherRequest.getCypher());
//            while (values.hasNext()){
//                Record.Builder recordBuilder = Record.newBuilder();
//                Value record = values.next();
//                recordBuilder.setRecordName(record.getType()).build();
//                Any.Builder resultBuilder = recordBuilder.getResultBuilder();
//
//
//                internalRecordsBuilder.addRecord(recordBuilder);
//            }
//            responseObserver.onNext(internalRecordsBuilder.build());
//            responseObserver.onCompleted();
//        }
//    }
//}