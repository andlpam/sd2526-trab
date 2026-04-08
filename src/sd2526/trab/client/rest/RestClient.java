package sd2526.trab.client.rest;

import java.net.URI;
import java.util.function.Supplier;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.ProcessingException;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;

public class RestClient {
  private static final int MAX_RETRIES = 3;
  private static final long RETRY_SLEEP = 1000;
  protected WebTarget target;

  public RestClient(URI serverURI) {
    ClientConfig config = new ClientConfig();
    config.property(ClientProperties.READ_TIMEOUT, 5000);
    config.property(ClientProperties.CONNECT_TIMEOUT, 5000);
    Client client = ClientBuilder.newClient(config);
    this.target = client.target(serverURI);
  }

  protected <T> Result<T> reTry(Supplier<Result<T>> func) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        return func.get();
      } catch (ProcessingException x) {
        try {
          Thread.sleep(RETRY_SLEEP);
        } catch (InterruptedException ignored) {
        }
      } catch (Exception x) {
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  protected <T> Result<T> processResponse(Response r, Class<T> entityType) {
    try {
      var status = r.getStatusInfo().toEnum();
      if (status == Status.OK && r.hasEntity())
        return Result.ok(r.readEntity(entityType));
      if (status == Status.NO_CONTENT)
        return Result.ok();
      return Result.error(getErrorCodeFrom(status.getStatusCode()));
    } finally {
      r.close();
    }
  }

  protected <T> Result<T> processResponse(Response r, GenericType<T> entityType) {
    try {
      var status = r.getStatusInfo().toEnum();
      if (status == Status.OK && r.hasEntity()) {
        return Result.ok(r.readEntity(entityType));
      }
      if (status == Status.NO_CONTENT) {
        return Result.ok();
      }
      return Result.error(getErrorCodeFrom(status.getStatusCode()));
    } finally {
      r.close();
    }
  }

  protected ErrorCode getErrorCodeFrom(int status) {
    return switch (status) {
      case 200, 204 -> ErrorCode.OK;
      case 409 -> ErrorCode.CONFLICT;
      case 403 -> ErrorCode.FORBIDDEN;
      case 404 -> ErrorCode.NOT_FOUND;
      case 400 -> ErrorCode.BAD_REQUEST;
      case 501 -> ErrorCode.NOT_IMPLEMENTED;
      default -> ErrorCode.INTERNAL_ERROR;
    };
  }
}