package sd2526.trab.client;

import java.net.URI;
import java.util.function.Function;
import sd2526.trab.discovery.Discovery;

public class ClientFactory<T> {
  final String serviceName;
  final Function<URI, T> restClientFunc;
  final Function<URI, T> grpcClientFunc;

  public ClientFactory(String serviceName, Function<URI, T> restClientFunc, Function<URI, T> grpcClientFunc) {
    this.serviceName = serviceName;
    this.restClientFunc = restClientFunc;
    this.grpcClientFunc = grpcClientFunc;
  }

  public T get(Discovery discovery, String domain) {
    URI[] uris = discovery.knownUrisOf(serviceName, domain, 1).value();

    if (uris == null || uris.length == 0) {
      throw new RuntimeException("No URIs found for service: " + serviceName);
    }

    URI uri = uris[0];

    if (uri.getScheme().equalsIgnoreCase("http"))
      return restClientFunc.apply(uri);
    if (uri.getScheme().equalsIgnoreCase("grpc"))
      return grpcClientFunc.apply(uri);

    throw new RuntimeException("Unknown scheme");
  }
}