package sd2526.trab.discovery;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;
import sd2526.trab.api.java.*;
import sd2526.trab.api.java.Result.ErrorCode;

/**
 * <p>
 * A class to perform service discovery, based on periodic service contact
 * endpoint announcements over multicast communication.
 * </p>
 * *
 * <p>
 * Servers announce their *name* and contact *uri* at regular intervals. The
 * server actively collects received announcements.
 * </p>
 * *
 * <p>
 * Service announcements have the following format:
 * </p>
 * *
 * <p>
 * &lt;service-name&gt;@&lt;domain-name&gt;&lt;delimiter-char&gt;&lt;server-uri&gt;
 * </p>
 */
public class Discovery {
  private static Logger Log = Logger.getLogger(Discovery.class.getName());
  private static final int MAX_RETRIES = 3;

  static {
    // addresses some multicast issues on some TCP/IP stacks
    System.setProperty("java.net.preferIPv4Stack", "true");
    // summarizes the logging format
    System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
  }

  // The pre-agreed multicast endpoint assigned to perform discovery.
  // Allowed IP Multicast range: 224.0.0.1 - 239.255.255.255
  static final public InetSocketAddress DISCOVERY_ADDR = new InetSocketAddress("226.226.226.226", 2266);
  static final int DISCOVERY_ANNOUNCE_PERIOD = 1000;
  static final int DISCOVERY_RETRY_TIMEOUT = 5000;
  static final int MAX_DATAGRAM_SIZE = 65536;

  // Used separate the two fields that make up a service announcement.
  private static final String DELIMITER = "\t";

  private final InetSocketAddress addr;
  private final String serviceName;
  private final String domain; // NOVO: O domínio do serviço
  private final String serviceURI;
  private final MulticastSocket ms;

  // O mapa agora guarda chaves no formato "Servico@Dominio"
  private final Map<String, Map<URI, Long>> knownServices = new ConcurrentHashMap<>();

  /**
   * @param addr        multicast address
   * @param serviceName the name of the service to announce (e.g., "Users",
   *                    "Messages")
   * @param domain      the domain of the service (e.g., "fct", "fc")
   * @param serviceURI  an uri string representing the contact endpoint
   */
  public Discovery(InetSocketAddress addr, String serviceName, String domain, String serviceURI)
      throws SocketException, UnknownHostException, IOException {
    this.addr = addr;
    this.serviceName = serviceName;
    this.domain = domain;
    this.serviceURI = serviceURI;

    if (this.addr == null) {
      throw new RuntimeException("A multinet address has to be provided.");
    }

    this.ms = new MulticastSocket(addr.getPort());
    this.ms.joinGroup(addr, NetworkInterface.getByInetAddress(InetAddress.getLocalHost()));
  }

  /**
   * Construtor para clientes (que apenas querem ouvir a rede e não anunciam)
   */
  public Discovery(InetSocketAddress addr) throws SocketException, UnknownHostException, IOException {
    this(addr, null, null, null);
  }

  /**
   * Starts sending service announcements at regular intervals and collects
   * incoming ones.
   */
  public void start() {
    // If this discovery instance was initialized with information about a service,
    // start the thread that makes the periodic announcement to the multicast
    // address.
    if (this.serviceName != null && this.domain != null && this.serviceURI != null) {

      Log.info(String.format("Starting Discovery announcements on: %s for: %s@%s -> %s", addr, serviceName, domain,
          serviceURI));

      // <service-name>@<domain-name><tab><server-uri>
      byte[] announceBytes = String.format("%s@%s%s%s", serviceName, domain, DELIMITER, serviceURI).getBytes();
      DatagramPacket announcePkt = new DatagramPacket(announceBytes, announceBytes.length, addr);

      try {
        new Thread(() -> {
          for (;;) {
            try {
              ms.send(announcePkt);
              Thread.sleep(DISCOVERY_ANNOUNCE_PERIOD);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }).start();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // start thread to collect announcements received from the network.
    new Thread(() -> {
      DatagramPacket pkt = new DatagramPacket(new byte[MAX_DATAGRAM_SIZE], MAX_DATAGRAM_SIZE);
      for (;;) {
        try {
          pkt.setLength(MAX_DATAGRAM_SIZE);
          ms.receive(pkt);
          String msg = new String(pkt.getData(), 0, pkt.getLength());
          String[] msgElems = msg.split(DELIMITER);

          if (msgElems.length == 2) {
            // msgElems[0] será algo como "Users@fct"
            // msgElems[1] será o URI "http://..."
            String recServiceAndDomain = msgElems[0];
            String recServiceURI = msgElems[1];

            try {
              URI uri = new URI(recServiceURI);
              // Updates the URI's respective to that specific domain
              knownServices.computeIfAbsent(recServiceAndDomain, k -> new ConcurrentHashMap<>())
                  .put(uri, System.currentTimeMillis());
            } catch (URISyntaxException e) {
              e.printStackTrace();
            }
          }
        } catch (IOException e) {
          // do nothing
        }
      }
    }).start();
  }

  /**
   * Returns the known services for a specific domain.
   * * @param serviceName the name of the service (e.g., "Users")
   * 
   * @param domain     the domain of the service (e.g., "fct")
   * @param minReplies minimum number of requested URIs. Blocks until satisfied.
   * @return an array of URI with the service instances discovered.
   */
  public Result<URI[]> knownUrisOf(String serviceName, String domain, int minReplies) {
    String searchKey = serviceName + "@" + domain;

    for (int i = 0; i < MAX_RETRIES; i++) {
      Map<URI, Long> instances = knownServices.getOrDefault(searchKey, new ConcurrentHashMap<>());
      long now = System.currentTimeMillis();

      // Choose the best server to communicate with
      List<URI> activeUris = instances.entrySet().stream()
          .filter(entry -> (now - entry.getValue()) <= DISCOVERY_RETRY_TIMEOUT)
          .map(Map.Entry::getKey)
          .collect(Collectors.toList());

      if (activeUris.size() >= minReplies) {
        return Result.ok(activeUris.toArray(new URI[0]));
      }
      try {
        Thread.sleep(DISCOVERY_RETRY_TIMEOUT);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return Result.ok(new URI[0]);
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  // Main just for testing purposes
  public static void main(String[] args) throws Exception {
    Discovery discovery = new Discovery(DISCOVERY_ADDR, "Users", "local",
        "http://" + InetAddress.getLocalHost().getHostAddress());
    discovery.start();
  }
}