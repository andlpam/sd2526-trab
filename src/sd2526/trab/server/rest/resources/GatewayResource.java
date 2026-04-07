package sd2526.trab.server.rest.resources;

import sd2526.trab.client.rest.RestMessagesClient;
import sd2526.trab.client.rest.RestUsersClient;
import sd2526.trab.client.grpc.GrpcUsersClient;
import sd2526.trab.client.grpc.GrpcMessagesClient;
import sd2526.trab.discovery.Discovery;
import sd2526.trab.api.java.*;
import sd2526.trab.api.rest.RestGateway;
import java.util.logging.Logger;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.api.*;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.List;

public class GatewayResource implements RestGateway {

  private String serviceName;
  private String domain;
  private Discovery discovery;
  private Messages localMessagesClient;
  private Users localUsersClient;
  private static Logger Log = Logger.getLogger(GatewayResource.class.getName());

  public GatewayResource(String serviceName, String domain, Discovery discovery) {
    this.serviceName = serviceName;
    this.domain = domain;
    this.discovery = discovery;
    this.localMessagesClient = null;
    this.localUsersClient = null;
  }

  private synchronized Users getLocalUsersClient() {
    if (localUsersClient != null) {
      return localUsersClient;
    }

    // First time searching for users client
    String serviceName = "Users";
    try {
      Result<URI[]> uris = discovery.knownUrisOf(serviceName, this.domain, 1);

      if (!uris.isOK()) {
        // This might be wrong, fix later
        // Timeout if server not being able to answer 5 seconds later
        Log.info("Server is not being able to delivering uris by multicast.");
        throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);
      }

      URI uri = uris.value()[0];
      if (uri.getScheme().equals("grpc")) {
        localUsersClient = new GrpcUsersClient(uri);
      } else {
        localUsersClient = new RestUsersClient(uri);
      }
      return localUsersClient;

    } catch (WebApplicationException w) {
      // Deixamos passar o 503 para cima
      throw w;
    } catch (Exception e) {
      // Cortamos o mal pela raiz! Se der erro grave, atiramos 500 em vez de devolver
      // null
      e.printStackTrace();
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }
  }

  private synchronized Messages getLocalMessagesClient() {
    if (this.localMessagesClient != null) {
      return this.localMessagesClient;
    }

    // First time searching for users client
    String serviceName = "Messages";
    try {
      Result<URI[]> uris = discovery.knownUrisOf(serviceName, this.domain, 1);

      if (!uris.isOK()) {
        // This might be wrong, fix later
        // Timeout if server not being able to answer 5 seconds later
        Log.info("Server is not being able to delivering uris by multicast.");
        throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);
      }

      URI uri = uris.value()[0];
      if (uri.getScheme().equals("grpc")) {
        this.localMessagesClient = new GrpcMessagesClient(uri);
      } else {
        localMessagesClient = new RestMessagesClient(uri);
      }

      return localMessagesClient;

    } catch (WebApplicationException w) {
      // Deixamos passar o 503 para cima
      throw w;
    } catch (Exception e) {
      // Cortamos o mal pela raiz! Se der erro grave, atiramos 500 em vez de devolver
      // null
      e.printStackTrace();
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }
  }

  @Override
  public String postUser(User user) {
    Users userClient = getLocalUsersClient();

    Result<String> result = userClient.postUser(user);

    if (result.isOK()) {
      return result.value();
    } else {
      throw new WebApplicationException(getStatusFromErrorCode(result.error()));
    }
  }

  @Override
  public User getUser(String name, String pwd) {
    Users userClient = getLocalUsersClient();
    Result<User> result = userClient.getUser(name, pwd);

    if (result.isOK()) {
      return result.value();
    } else {
      throw new WebApplicationException(getStatusFromErrorCode(result.error()));
    }
  }

  @Override
  public User updateUser(String name, String pwd, User info) {
    Users userClient = getLocalUsersClient();
    Result<User> result = userClient.updateUser(name, pwd, info);

    if (result.isOK()) {
      return result.value();
    } else {
      throw new WebApplicationException(getStatusFromErrorCode(result.error()));
    }
  }

  @Override
  public User deleteUser(String name, String pwd) {
    Users userClient = getLocalUsersClient();
    Result<User> result = userClient.deleteUser(name, pwd);

    if (result.isOK()) {
      return result.value();
    } else {
      throw new WebApplicationException(getStatusFromErrorCode(result.error()));
    }
  }

  @Override
  public List<User> searchUsers(String name, String pwd, String pattern) {
    Users userClient = getLocalUsersClient();
    Result<List<User>> result = userClient.searchUsers(name, pwd, pattern);

    if (result.isOK()) {
      return result.value();
    } else {
      throw new WebApplicationException(getStatusFromErrorCode(result.error()));
    }
  }

  @Override
  public String postMessage(String pwd, Message msg) {
    Messages messageClient = getLocalMessagesClient();
    Result<String> result = messageClient.postMessage(pwd, msg);

    if (result.isOK()) {
      return result.value();
    } else {
      throw new WebApplicationException(getStatusFromErrorCode(result.error()));
    }
  }

  @Override
  public Message getMessage(String name, String mid, String pwd) {
    Messages messageClient = getLocalMessagesClient();
    Result<Message> result = messageClient.getInboxMessage(name, mid, pwd);

    if (result.isOK()) {
      return result.value();
    } else {
      throw new WebApplicationException(getStatusFromErrorCode(result.error()));
    }
  }

  @Override
  public List<String> getMessages(String name, String pwd, String query) {
    Messages messageClient = getLocalMessagesClient();
    Result<List<String>> result = messageClient.searchInbox(name, pwd, query);

    if (result.isOK()) {
      return result.value();
    } else {
      throw new WebApplicationException(getStatusFromErrorCode(result.error()));
    }
  }

  @Override
  public void removeFromUserInbox(String name, String mid, String pwd) {
    Messages messageClient = getLocalMessagesClient();
    Result<Void> result = messageClient.removeInboxMessage(name, mid, pwd);

    if (result.isOK()) {
      return;
    } else {
      throw new WebApplicationException(getStatusFromErrorCode(result.error()));
    }
  }

  @Override
  public void deleteMessage(String name, String mid, String pwd) {
    Messages messageClient = getLocalMessagesClient();
    Result<Void> result = messageClient.deleteMessage(name, mid, pwd);

    if (result.isOK()) {
      return;
    } else {
      throw new WebApplicationException(getStatusFromErrorCode(result.error()));
    }
  }

  public static Status getStatusFromErrorCode(ErrorCode error) {
    return switch (error) {
      case OK -> Status.OK;
      case CONFLICT -> Status.CONFLICT;
      case FORBIDDEN -> Status.FORBIDDEN;
      case NOT_FOUND -> Status.NOT_FOUND;
      case BAD_REQUEST -> Status.BAD_REQUEST;
      case INTERNAL_ERROR -> Status.INTERNAL_SERVER_ERROR;
      case NOT_IMPLEMENTED -> Status.NOT_IMPLEMENTED;
      case TIMEOUT -> Status.REQUEST_TIMEOUT;
      default -> Status.INTERNAL_SERVER_ERROR;
    };
  }
}
