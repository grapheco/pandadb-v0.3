package cn.pandadb.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;

public class PandaServer {
    private Server server;

    public void start(int port) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new PandaServiceImpl()).build().start();

        System.out.println("started...");
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down pandaRpc server since JVM is shutting down");
                PandaServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop(){
        if (server != null){
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final PandaServer server = new PandaServer();
        server.start(50010);
        System.out.println("started...");
        server.blockUntilShutdown();
    }

    public static class PandaServiceImpl extends PandaRpcServiceGrpc.PandaRpcServiceImplBase{
        @Override
        public void getInternalResult(CypherRequest cypher, StreamObserver<InternalRecords> responseObserver) {
            Record record = Record.newBuilder().setRecordName("test").build();
            InternalRecords records = InternalRecords.newBuilder().addRecord(record).build();
            responseObserver.onNext(records);
            responseObserver.onCompleted();
        }
    }
}
