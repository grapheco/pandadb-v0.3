import cn.pandadb.grpc.CypherRequest;
import cn.pandadb.grpc.InternalRecords;
import cn.pandadb.grpc.PandaRpcServiceGrpc;
import cn.pandadb.grpc.Record;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;

public class PandaRpcServer {
    private Server server;
    public void start(int port) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new PandaRpcServer.PandaServiceImpl()).build().start();

        System.out.println("started...");
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down pandaRpc server since JVM is shutting down");
                PandaRpcServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        PandaRpcServer server = new PandaRpcServer();
        server.start(50010);
        System.out.println("started...");
        server.blockUntilShutdown();
    }

    private static class PandaServiceImpl extends PandaRpcServiceGrpc.PandaRpcServiceImplBase {

        @Override
        public void getInternalResult(CypherRequest cypher, StreamObserver<InternalRecords> responseObserver) {
            System.out.println("get...." + cypher.getCypher());
            Record record = Record.newBuilder().setRecordName("Hello~~Client!!!").build();
            InternalRecords records = InternalRecords.newBuilder().addRecord(record).build();
            responseObserver.onNext(records);
            responseObserver.onCompleted();
        }
    }
}