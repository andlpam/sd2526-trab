package sd2526.trab.client.grpc;

import io.grpc.StatusRuntimeException;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import java.util.function.Supplier;

public class GrpcClient {
  protected static final int MAX_RETRIES = 3;
  protected static final int RETRY_SLEEP = 1000;

  protected <T> Result<T> reTry(Supplier<Result<T>> func) {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        return func.get();
      } catch (StatusRuntimeException e) {
        if (isTransientError(e)) {
          try {
            Thread.sleep(RETRY_SLEEP);
          } catch (InterruptedException ignored) {
          }
        } else {
          return Result.error(getErrorCodeFrom(e));
        }
      } catch (Exception e) {
        e.printStackTrace(); // Adiciona esta linha!
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  private boolean isTransientError(StatusRuntimeException e) {
    return e.getStatus().getCode() == io.grpc.Status.Code.UNAVAILABLE ||
        e.getStatus().getCode() == io.grpc.Status.Code.DEADLINE_EXCEEDED;
  }

  protected ErrorCode getErrorCodeFrom(StatusRuntimeException e) {
    return switch (e.getStatus().getCode()) {
      case OK -> ErrorCode.OK;
      case ALREADY_EXISTS -> ErrorCode.CONFLICT;
      case PERMISSION_DENIED -> ErrorCode.FORBIDDEN;
      case NOT_FOUND -> ErrorCode.NOT_FOUND;
      case INVALID_ARGUMENT -> ErrorCode.BAD_REQUEST;
      case UNIMPLEMENTED -> ErrorCode.NOT_IMPLEMENTED;
      case UNAVAILABLE, DEADLINE_EXCEEDED -> ErrorCode.TIMEOUT;
      default -> ErrorCode.INTERNAL_ERROR;
    };
  }
}