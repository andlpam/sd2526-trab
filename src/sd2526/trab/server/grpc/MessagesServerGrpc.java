package sd2526.trab.server.grpc;

import java.net.InetAddress;
import java.util.logging.Logger;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import sd2526.trab.discovery.Discovery;
import sd2526.trab.server.grpc.controllers.GrpcMessagesController;
import sd2526.trab.server.grpc.controllers.GrpcUsersController;

public class MessagesServerGrpc {

  public static final int PORT = 9000;
  private static final String GRPC_CTX = "/grpc";
  public static final String SERVICE_NAME = "Messages";
  private static final String SERVER_BASE_URI = "grpc://%s:%s%s";
  private static Logger Log = Logger.getLogger(UsersServerGrpc.class.getName());

  public static void main(String[] args) throws Exception {
    String hostname = InetAddress.getLocalHost().getHostName();
    String domain = "";
    if (hostname.contains(".")) {
      domain = hostname.substring(hostname.indexOf('.') + 1);
    }
    String ip = InetAddress.getLocalHost().getHostAddress();
    String serverURI = String.format(SERVER_BASE_URI, InetAddress.getLocalHost(), PORT, GRPC_CTX);

    Discovery discovery = new Discovery(Discovery.DISCOVERY_ADDR, SERVICE_NAME, domain, serverURI);
    discovery.start();
    GrpcMessagesController stub = new GrpcMessagesController(domain, discovery);
    ServerCredentials cred = InsecureServerCredentials.create();
    Server server = Grpc.newServerBuilderForPort(PORT, cred).addService(stub).build();

    Log.info(String.format("Users gRPC Server ready @ %s\n", serverURI));
    server.start().awaitTermination();
  }
}
