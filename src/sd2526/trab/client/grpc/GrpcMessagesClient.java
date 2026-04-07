package sd2526.trab.client.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import sd2526.trab.api.Message;
import sd2526.trab.api.grpc.GrpcMessagesGrpc;
import sd2526.trab.api.grpc.Messages.*;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.server.grpc.utils.DataModelAdaptor;

import java.net.URI;
import java.util.List;
import java.util.logging.Logger;

public class GrpcMessagesClient implements Messages {
  private static Logger Log = Logger.getLogger(GrpcMessagesClient.class.getName());

  protected static final int MAX_RETRIES = 3;
  protected static final int RETRY_SLEEP = 5000;

  final URI serverURI;
  final ManagedChannel channel;
  final GrpcMessagesGrpc.GrpcMessagesBlockingStub stub;

  public GrpcMessagesClient(URI serverUri) {
    this.serverURI = serverUri;
    // O gRPC usa um canal em vez de um cliente HTTP
    this.channel = ManagedChannelBuilder.forAddress(serverURI.getHost(), serverURI.getPort())
        .usePlaintext() // Assume-se que não estamos a usar TLS para os testes locais
        .build();
    // Usamos o stub bloqueante (síncrono) para o cliente
    this.stub = GrpcMessagesGrpc.newBlockingStub(channel);
  }

  @Override
  public Result<String> postMessage(String pwd, Message msg) {
    PostMessageArgs request = PostMessageArgs.newBuilder()
        .setPwd(pwd)
        .setMessage(DataModelAdaptor.Message_to_GrpcMessage(msg))
        .build();

    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        PostMessageResult response = stub.postMessage(request);
        return Result.ok(response.getMid());
      } catch (StatusRuntimeException e) {
        if (isTransientError(e)) {
          Log.info("Retry " + (i + 1) + " due to: " + e.getStatus().getDescription());
          sleep();
        } else {
          return Result.error(getErrorCodeFrom(e));
        }
      } catch (Exception e) {
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<Message> getInboxMessage(String name, String mid, String pwd) {
    GetInboxMessageArgs request = GetInboxMessageArgs.newBuilder()
        .setName(name)
        .setMid(mid)
        .setPwd(pwd)
        .build();

    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        GrpcMessage response = stub.getInboxMessage(request);
        return Result.ok(DataModelAdaptor.GrpcMessage_to_Message(response));
      } catch (StatusRuntimeException e) {
        if (isTransientError(e)) {
          Log.info("Retry " + (i + 1) + " due to: " + e.getStatus().getDescription());
          sleep();
        } else {
          return Result.error(getErrorCodeFrom(e));
        }
      } catch (Exception e) {
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<List<String>> getAllInboxMessages(String name, String pwd) {
    GetAllInboxMessagesArgs request = GetAllInboxMessagesArgs.newBuilder()
        .setName(name)
        .setPwd(pwd)
        .build();

    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        GetAllInboxMessagesResult response = stub.getAllInboxMessages(request);
        return Result.ok(response.getMidsList());
      } catch (StatusRuntimeException e) {
        if (isTransientError(e)) {
          Log.info("Retry " + (i + 1) + " due to: " + e.getStatus().getDescription());
          sleep();
        } else {
          return Result.error(getErrorCodeFrom(e));
        }
      } catch (Exception e) {
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<List<String>> searchInbox(String name, String pwd, String query) {
    SearchInboxArgs request = SearchInboxArgs.newBuilder()
        .setName(name)
        .setPwd(pwd)
        .setQuery(query)
        .build();

    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        SearchInboxResult response = stub.searchInbox(request);
        return Result.ok(response.getMidsList());
      } catch (StatusRuntimeException e) {
        if (isTransientError(e)) {
          Log.info("Retry " + (i + 1) + " due to: " + e.getStatus().getDescription());
          sleep();
        } else {
          return Result.error(getErrorCodeFrom(e));
        }
      } catch (Exception e) {
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
    RemoveInboxMessageArgs request = RemoveInboxMessageArgs.newBuilder()
        .setName(name)
        .setMid(mid)
        .setPwd(pwd)
        .build();

    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        stub.removeInboxMessage(request);
        return Result.ok();
      } catch (StatusRuntimeException e) {
        if (isTransientError(e)) {
          Log.info("Retry " + (i + 1) + " due to: " + e.getStatus().getDescription());
          sleep();
        } else {
          return Result.error(getErrorCodeFrom(e));
        }
      } catch (Exception e) {
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<Void> deleteMessage(String name, String mid, String pwd) {
    DeleteMessageArgs request = DeleteMessageArgs.newBuilder()
        .setName(name)
        .setMid(mid)
        .setPwd(pwd)
        .build();

    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        stub.deleteMessage(request);
        return Result.ok();
      } catch (StatusRuntimeException e) {
        if (isTransientError(e)) {
          Log.info("Retry " + (i + 1) + " due to: " + e.getStatus().getDescription());
          sleep();
        } else {
          return Result.error(getErrorCodeFrom(e));
        }
      } catch (Exception e) {
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  private void sleep() {
    try {
      Thread.sleep(RETRY_SLEEP);
    } catch (InterruptedException ignored) {
    }
  }

  private boolean isTransientError(StatusRuntimeException e) {
    return e.getStatus().getCode() == io.grpc.Status.Code.UNAVAILABLE ||
        e.getStatus().getCode() == io.grpc.Status.Code.DEADLINE_EXCEEDED;
  }

  public static ErrorCode getErrorCodeFrom(StatusRuntimeException e) {
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