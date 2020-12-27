package cn.pandadb.driver.utils;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import scala.Int;

public class TransferGrpcToScala {
    public static ManagedChannel getChannel(int port){
       return ManagedChannelBuilder.forTarget("localhost:" + port).usePlaintext().build();
    }
}
