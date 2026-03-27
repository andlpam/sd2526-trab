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
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.api.rest.RestUsers;

public class RestMessagesClient {
  private static Logger Log = Logger.getLogger(RestUsersClient.class.getName());

  protected static final int READ_TIMEOUT = 5000;
  protected static final int CONNECT_TIMEOUT = 5000;

  protected static final int MAX_RETRIES = 3;
  protected static final int RETRY_SLEEP = 5000;

  final URI serverURI;
  final Client client;
  final ClientConfig config;
  final WebTarget target;

  public RestMessagesClient(URI serverUri) {
    this.serverURI = serverUri;
    this.config = new ClientConfig();

    // Create a config to deal with errors when the connection goes down
    config.property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT);
    config.property(ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);

    this.client = ClientBuilder.newClient(config);
    // Create a target to the server who is implementing that specific interface
    target = client.target(serverURI).path(RestUsers.PATH);
  }

  public Result<String> createMessage(String pwd, Message msg) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        Response r = target.queryParam(RestMessages.PWD, pwd)
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
          // Nothing to do here
        }
      } catch (Exception x) {
        x.getStackTrace();
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  public Result<Message> getMessage(String name, String mid, String pwd) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        Response r = target.path(RestMessages.MBOX)
            .path(name)
            .path(mid)
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
          // Nothing to do here
        }
      } catch (Exception x) {
        x.getStackTrace();
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  public Result<List<String>> getMessages(String name, String pwd, String query) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        // Idk if the default value here in the query must be provided but probably not
        Response r = target.path(RestMessages.MBOX)
            .path(name)
            .queryParam(RestMessages.PWD, pwd)
            .queryParam(RestMessages.QUERY, query)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .get();

        int status = r.getStatus();
        if (status == Status.OK.getStatusCode() && r.hasEntity())
          return Result.ok(r.readEntity(new GenericType<List<String>>() {
          }));
        else
          return Result.error(getErrorCodeFrom(status));
      } catch (ProcessingException e) {
        Log.info(e.getMessage());
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException x) {
          // Nothing to do here
        }
      } catch (Exception x) {
        x.getStackTrace();
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  public Result<Void> removeMessageFromInBox(String name, String mid, String pwd) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        // Idk if the default value here in the query must be provided but probably not
        Response r = target.path(RestMessages.MBOX)
            .path(name)
            .path(mid)
            .queryParam(RestMessages.PWD, pwd)
            .request()
            .delete();

        int status = r.getStatus();
        if (status == Status.OK.getStatusCode() && r.hasEntity())
          return Result.ok();
        else
          return Result.error(getErrorCodeFrom(status));
      } catch (ProcessingException e) {
        Log.info(e.getMessage());
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException x) {
          // Nothing to do here
        }
      } catch (Exception x) {
        x.getStackTrace();
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  public Result<Void> deleteMessageSent(String name, String mid, String pwd) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        // Idk if the default value here in the query must be provided but probably not
        Response r = target.path(name)
            .path(mid)
            .queryParam(RestMessages.PWD, pwd)
            .request()
            .delete();

        int status = r.getStatus();
        if (status == Status.OK.getStatusCode() && r.hasEntity())
          return Result.ok();
        else
          return Result.error(getErrorCodeFrom(status));
      } catch (ProcessingException e) {
        Log.info(e.getMessage());
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException x) {
          // Nothing to do here
        }
      } catch (Exception x) {
        x.getStackTrace();
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
