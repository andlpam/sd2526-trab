package sd2526.trab.client.grpc;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import sd2526.trab.api.User;
import sd2526.trab.api.grpc.GrpcUsersGrpc;
import sd2526.trab.api.grpc.Users.*;
import sd2526.trab.api.java.Users;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.server.grpc.utils.DataModelAdaptor;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

public class GrpcUsersClient implements Users {
  private static Logger Log = Logger.getLogger(GrpcUsersClient.class.getName());

  protected static final int MAX_RETRIES = 3;
  protected static final int RETRY_SLEEP = 5000;

  private final GrpcUsersGrpc.GrpcUsersBlockingStub stub;

  public GrpcUsersClient(URI serverUri) {
    Channel channel = ManagedChannelBuilder.forAddress(serverUri.getHost(), serverUri.getPort())
        .enableRetry()
        .usePlaintext()
        .build();
    this.stub = GrpcUsersGrpc.newBlockingStub(channel);
  }

  @Override
  public Result<String> postUser(User user) {
    GrpcUser grpcUser = DataModelAdaptor.User_to_GrpcUser(user);
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        PostUserResult res = stub.postUser(grpcUser);
        return Result.ok(res.getUserAddress());
      } catch (StatusRuntimeException sre) {
        if (isTransientError(sre))
          sleep();
        else
          return Result.error(statusToErrorCode(sre.getStatus()));
      } catch (Exception e) {
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<User> getUser(String name, String pwd) {
    GetUserArgs request = GetUserArgs.newBuilder()
        .setName(name)
        .setPwd(pwd)
        .build();

    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        GetUserResult res = stub.getUser(request);
        return Result.ok(DataModelAdaptor.GrpcUser_to_User(res.getUser()));
      } catch (StatusRuntimeException sre) {
        if (isTransientError(sre))
          sleep();
        else
          return Result.error(statusToErrorCode(sre.getStatus()));
      } catch (Exception e) {
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<User> updateUser(String name, String pwd, User info) {
    GrpcUser new_info = DataModelAdaptor.User_to_GrpcUser(info);
    UpdateUserArgs request = UpdateUserArgs.newBuilder()
        .setName(name)
        .setPwd(pwd)
        .setInfo(new_info).build();

    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        UpdateUserResult res = stub.updateUser(request);
        return Result.ok(DataModelAdaptor.GrpcUser_to_User(res.getUser()));
      } catch (StatusRuntimeException sre) {
        if (isTransientError(sre))
          sleep();
        else
          return Result.error(statusToErrorCode(sre.getStatus()));
      } catch (Exception e) {
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<User> deleteUser(String name, String pwd) {
    DeleteUserArgs request = DeleteUserArgs.newBuilder()
        .setName(name)
        .setPwd(pwd)
        .build();

    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        DeleteUserResult res = stub.deleteUser(request);
        return Result.ok(DataModelAdaptor.GrpcUser_to_User(res.getUser()));
      } catch (StatusRuntimeException sre) {
        if (isTransientError(sre))
          sleep();
        else
          return Result.error(statusToErrorCode(sre.getStatus()));
      } catch (Exception e) {
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
    return Result.error(ErrorCode.TIMEOUT);
  }

  @Override
  public Result<List<User>> searchUsers(String name, String pwd, String query) {
    SearchUsersArgs request = SearchUsersArgs.newBuilder()
        .setName(name)
        .setPwd(pwd)
        .setQuery(query)
        .build();

    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        Iterator<GrpcUser> res = stub.searchUsers(request);
        List<User> ret = new ArrayList<>();
        while (res.hasNext()) {
          ret.add(DataModelAdaptor.GrpcUser_to_User(res.next()));
        }
        return Result.ok(ret);
      } catch (StatusRuntimeException sre) {
        if (isTransientError(sre))
          sleep();
        else
          return Result.error(statusToErrorCode(sre.getStatus()));
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

  private Result.ErrorCode statusToErrorCode(io.grpc.Status status) {
    switch (status.getCode()) {
      case OK:
        return Result.ErrorCode.OK;
      case ALREADY_EXISTS:
        return Result.ErrorCode.CONFLICT;
      case PERMISSION_DENIED:
        return Result.ErrorCode.FORBIDDEN;
      case NOT_FOUND:
        return Result.ErrorCode.NOT_FOUND;
      case INVALID_ARGUMENT:
        return Result.ErrorCode.BAD_REQUEST;
      case UNIMPLEMENTED:
        return Result.ErrorCode.NOT_IMPLEMENTED;
      case UNAVAILABLE:
      case DEADLINE_EXCEEDED:
        return Result.ErrorCode.TIMEOUT;
      default:
        return Result.ErrorCode.INTERNAL_ERROR;
    }
  }
}