package sd2526.trab.server;

import java.net.InetAddress;
import java.net.URI;
import java.util.logging.Logger;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.discovery.Discovery;
import sd2526.trab.server.resources.MessagesResource;

public class MessagesServer {

  private static Logger Log = Logger.getLogger(MessagesServer.class.getName());
  public static final int PORT = 8080;
  public static final String SERVICE_NAME = "Messages";
  private static final String SERVER_URI_FMT = "http://%s:%s/rest";

  // Oficial rules for using Jersey
  static {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
  }

  public static void main(String[] args) {

    try {
      ResourceConfig config = new ResourceConfig();

      // can be messages.fct, messages.fcsh etc..
      String hostname = InetAddress.getLocalHost().getHostName();
      // Get the cur host domain
      String domain = "";
      if (hostname.contains(".")) {
        domain = hostname.substring(hostname.indexOf('.') + 1);
      }
      String ip = InetAddress.getLocalHost().getHostAddress();
      String serverURI = String.format(SERVER_URI_FMT, ip, PORT);

      // Debugging
      Log.info(String.format("%s Server ready @ %s\n", SERVICE_NAME, serverURI));
      // Create the discovery and start the threads
      Discovery discovery = new Discovery(Discovery.DISCOVERY_ADDR, ip, domain, serverURI);
      discovery.start();
      // Server resource need the discovery mechanism..
      MessagesResource res = new MessagesResource(SERVICE_NAME, domain, discovery);
      config.register(res);
      // Load the server
      JdkHttpServerFactory.createHttpServer(URI.create(serverURI), config);

    } catch (Exception e) {
      Log.severe(e.getMessage());
    }

  }

}