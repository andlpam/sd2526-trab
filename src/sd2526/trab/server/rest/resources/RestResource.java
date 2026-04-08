package sd2526.trab.server.rest.resources;

import sd2526.trab.api.java.Result;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;

public class RestResource {

  protected static Status errorCodeToStatus(Result.ErrorCode error) {
    return switch (error) {
      case NOT_FOUND -> Status.NOT_FOUND;
      case CONFLICT -> Status.CONFLICT;
      case FORBIDDEN -> Status.FORBIDDEN;
      case NOT_IMPLEMENTED -> Status.NOT_IMPLEMENTED;
      case BAD_REQUEST -> Status.BAD_REQUEST;
      default -> Status.INTERNAL_SERVER_ERROR;
    };
  }

  protected static <T> T unwrapResultOrThrow(Result<T> result) {
    if (result.isOK()) {
      return result.value();
    } else {
      throw new WebApplicationException(errorCodeToStatus(result.error()));
    }
  }
}