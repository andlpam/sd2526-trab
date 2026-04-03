package sd2526.trab.client.rest;

import java.net.URI;
import java.util.List;
import java.util.logging.Logger;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import sd2526.trab.api.*;
import sd2526.trab.api.java.*;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.api.rest.RestGateway;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.api.rest.RestUsers;

public class RestGatewayClient implements Users, Messages {
  private static Logger Log = Logger.getLogger(RestGatewayClient.class.getName());

  protected static final int READ_TIMEOUT = 5000;
  protected static final int CONNECT_TIMEOUT = 5000;

  protected static final int MAX_RETRIES = 3;
  protected static final int RETRY_SLEEP = 5000;

  final URI serverURI;
  final Client client;
  final ClientConfig config;
  final WebTarget target;

  public RestGatewayClient(URI serverUri) {
    this.serverURI = serverUri;
    this.config = new ClientConfig();

    config.property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT);
    config.property(ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);

    this.client = ClientBuilder.newClient(config);
    // O Gateway targeta a raiz ("/") baseada na tua interface RestGateway
    target = client.target(serverURI).path(RestGateway.PATH);
  }

  // ==========================================
  // USERS OPERATIONS
  // ==========================================

  @Override
  public Result<String> postUser(User user) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        Response r = target.path("users")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(user, MediaType.APPLICATION_JSON));

        int status = r.getStatus();
        if (status != Status.OK.getStatusCode())
          return Result.error(getErrorCodeFrom(status));
        else
          return Result.ok(r.readEntity(String.class));

      } catch (ProcessingException x) {
        Log.info(x.getMessage());
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException e) {
        }
      } catch (Exception x) {
        x.printStackTrace();
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<User> getUser(String name, String pwd) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        Response r = target.path("users").path(name)
            .queryParam(RestUsers.PWD, pwd)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .get();

        int status = r.getStatus();
        if (status != Status.OK.getStatusCode())
          return Result.error(getErrorCodeFrom(status));
        else
          return Result.ok(r.readEntity(User.class));
      } catch (ProcessingException x) {
        Log.info(x.getMessage());
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException e) {
        }
      } catch (Exception x) {
        x.printStackTrace();
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<User> updateUser(String name, String pwd, User info) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        Response r = target.path("users").path(name)
            .queryParam(RestUsers.PWD, pwd)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .put(Entity.entity(info, MediaType.APPLICATION_JSON));

        int status = r.getStatus();
        if (status != Status.OK.getStatusCode())
          return Result.error(getErrorCodeFrom(status));
        else
          return Result.ok(r.readEntity(User.class));

      } catch (ProcessingException e) {
        Log.info(e.getMessage());
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException x) {
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<User> deleteUser(String name, String pwd) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        Response r = target.path("users").path(name)
            .queryParam(RestUsers.PWD, pwd)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .delete();

        int status = r.getStatus();
        if (status != Status.OK.getStatusCode())
          return Result.error(getErrorCodeFrom(status));
        else
          return Result.ok(r.readEntity(User.class));

      } catch (ProcessingException e) {
        Log.info(e.getMessage());
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException x) {
        }
      } catch (Exception x) {
        x.printStackTrace();
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<List<User>> searchUsers(String name, String pwd, String pattern) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        Response r = target.path("users")
            .queryParam(RestUsers.NAME, name)
            .queryParam(RestUsers.PWD, pwd)
            .queryParam(RestUsers.QUERY, pattern)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .get();

        int status = r.getStatus();
        if (status != Status.OK.getStatusCode())
          return Result.error(getErrorCodeFrom(status));
        else
          return Result.ok(r.readEntity(new GenericType<List<User>>() {
          }));

      } catch (ProcessingException e) {
        Log.info(e.getMessage());
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException x) {
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  // ==========================================
  // MESSAGES OPERATIONS
  // ==========================================

  @Override
  public Result<String> postMessage(String pwd, Message msg) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        Response r = target.path("messages")
            .queryParam(RestMessages.PWD, pwd)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(msg, MediaType.APPLICATION_JSON));

        int status = r.getStatus();
        if (status == Status.OK.getStatusCode() && r.hasEntity())
          return Result.ok(r.readEntity(String.class));
        else
          return Result.error(getErrorCodeFrom(status));
      } catch (ProcessingException e) {
        Log.info(e.getMessage());
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException x) {
        }
      } catch (Exception x) {
        x.printStackTrace();
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<Message> getInboxMessage(String name, String mid, String pwd) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        Response r = target.path("messages").path("mbox").path(name).path(mid)
            .queryParam(RestMessages.PWD, pwd)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .get();

        int status = r.getStatus();
        if (status == Status.OK.getStatusCode() && r.hasEntity())
          return Result.ok(r.readEntity(Message.class));
        else
          return Result.error(getErrorCodeFrom(status));
      } catch (ProcessingException e) {
        Log.info(e.getMessage());
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException x) {
        }
      } catch (Exception x) {
        x.printStackTrace();
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<List<String>> getAllInboxMessages(String name, String pwd) {
    return searchInbox(name, pwd, "");
  }

  @Override
  public Result<List<String>> searchInbox(String name, String pwd, String query) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        Response r = target.path("messages").path("mbox").path(name)
            .queryParam(RestMessages.PWD, pwd)
            .queryParam(RestMessages.QUERY, query)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .get();

        int status = r.getStatus();
        if (status == Status.OK.getStatusCode() && r.hasEntity()) {
          return Result.ok(r.readEntity(new GenericType<List<String>>() {
          }));
        } else {
          return Result.error(getErrorCodeFrom(status));
        }
      } catch (ProcessingException e) {
        Log.info("Retry " + (i + 1) + " due to: " + e.getMessage());
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException x) {
        }
      } catch (Exception x) {
        x.printStackTrace();
        break;
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        Response r = target.path("messages").path("mbox").path(name).path(mid)
            .queryParam(RestMessages.PWD, pwd)
            .request()
            .delete();

        int status = r.getStatus();
        if (status == Status.OK.getStatusCode() || status == Status.NO_CONTENT.getStatusCode())
          return Result.ok();
        else
          return Result.error(getErrorCodeFrom(status));
      } catch (ProcessingException e) {
        Log.info(e.getMessage());
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException x) {
        }
      } catch (Exception x) {
        x.printStackTrace();
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<Void> deleteMessage(String name, String mid, String pwd) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        Response r = target.path("messages").path(name).path(mid)
            .queryParam(RestMessages.PWD, pwd)
            .request()
            .delete();

        int status = r.getStatus();
        if (status == Status.OK.getStatusCode() || status == Status.NO_CONTENT.getStatusCode())
          return Result.ok();
        else
          return Result.error(getErrorCodeFrom(status));
      } catch (ProcessingException e) {
        Log.info(e.getMessage());
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException x) {
        }
      } catch (Exception x) {
        x.printStackTrace();
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  public static ErrorCode getErrorCodeFrom(int status) {
    return switch (status) {
      case 200, 209 -> ErrorCode.OK;
      case 409 -> ErrorCode.CONFLICT;
      case 403 -> ErrorCode.FORBIDDEN;
      case 404 -> ErrorCode.NOT_FOUND;
      case 400 -> ErrorCode.BAD_REQUEST;
      case 500 -> ErrorCode.INTERNAL_ERROR;
      case 501 -> ErrorCode.NOT_IMPLEMENTED;
      default -> ErrorCode.INTERNAL_ERROR;
    };
  }
}