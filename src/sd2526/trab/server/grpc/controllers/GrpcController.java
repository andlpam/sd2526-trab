package sd2526.trab.server.grpc.controllers;

import io.grpc.Status;
import sd2526.trab.api.java.Result;
import io.grpc.stub.StreamObserver;
import java.util.function.Function;

public class GrpcController {

  protected static Throwable errorCodeToStatus(Result.ErrorCode error) {
    var status = switch (error) {
      case NOT_FOUND -> Status.NOT_FOUND;
      case CONFLICT -> Status.ALREADY_EXISTS;
      case FORBIDDEN -> Status.PERMISSION_DENIED;
      case NOT_IMPLEMENTED -> Status.UNIMPLEMENTED;
      case BAD_REQUEST -> Status.INVALID_ARGUMENT;
      default -> Status.INTERNAL;
    };
    return status.asException();
  }

  // Função auxiliar para evitar escrever try/catch nos observers
  protected <T, R> void toGrpcResult(StreamObserver<R> observer, Result<T> result, Function<T, R> mapper) {
    if (result.isOK()) {
      observer.onNext(mapper.apply(result.value()));
      observer.onCompleted();
    } else {
      observer.onError(errorCodeToStatus(result.error()));
    }
  }
}