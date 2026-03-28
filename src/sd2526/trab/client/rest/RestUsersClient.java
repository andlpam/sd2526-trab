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
import sd2526.trab.api.rest.RestUsers;

public class RestUsersClient implements Users {
  private static Logger Log = Logger.getLogger(RestUsersClient.class.getName());

  protected static final int READ_TIMEOUT = 5000;
  protected static final int CONNECT_TIMEOUT = 5000;

  protected static final int MAX_RETRIES = 3;
  protected static final int RETRY_SLEEP = 5000;

  final URI serverURI;
  final Client client;
  final ClientConfig config;
  final WebTarget target;

  public RestUsersClient(URI serverUri) {
    this.serverURI = serverUri;
    this.config = new ClientConfig();

    // ERROR IN THIS FILE NEED TO CHECK IF IT HAS ENTITY IN EVERY SINGLE ONE

    // Create a config to deal with errors when the connection goes down
    config.property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT);
    config.property(ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);

    this.client = ClientBuilder.newClient(config);
    // Create a target to the server who is implementing that specific interface
    target = client.target(serverURI).path(RestUsers.PATH);
  }

  // In this method they know the associated Rest function in the REST server
  // because of the single input
  @Override
  public Result<String> postUser(User user) {

    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        Response r = target.request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(user, MediaType.APPLICATION_JSON));

        int status = r.getStatus();
        if (status != Status.OK.getStatusCode())
          return Result.error(getErrorCodeFrom(status));
        else
          return Result.ok(r.readEntity(String.class));

      } catch (ProcessingException x) {
        Log.info(x.getMessage());
        // Try again mechanism
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException e) {
          // Nothing to be done here.
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
        // To build the request first we need to put the query params and the path
        // params first
        Response r = target.path(name)
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
          // Nothing to be done here
        }
      } catch (Exception x) {
        x.printStackTrace();
      }
    }
    // If the response takes to long to be received or it is a zombie.
    return Result.error(ErrorCode.TIMEOUT);

  }

  @Override
  public Result<User> updateUser(String name, String pwd, User info) {
    for (int i = 0; i < MAX_RETRIES; i++) {

      try {
        Response r = target.path(name)
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
          // Nothing to be done here
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
        Response r = target.path(name)
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
          // Noting to be done here
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
        Response r = target.queryParam(RestUsers.NAME, name)
            .queryParam(RestUsers.PWD, pwd)
            .queryParam(RestUsers.QUERY, pattern)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .get();

        int status = r.getStatus();
        if (status != Status.OK.getStatusCode())
          return Result.error(getErrorCodeFrom(status));
        else
          // MIGHT NOT HAVE ENTITY SO WE NEED TO DEAL WITH THIS LATER
          return Result.ok(r.readEntity(new GenericType<List<User>>() {
          }));

      } catch (ProcessingException e) {
        Log.info(e.getMessage());
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException x) {
          // Do nothing here
        }
      } catch (Exception e) {
        e.getStackTrace();
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
