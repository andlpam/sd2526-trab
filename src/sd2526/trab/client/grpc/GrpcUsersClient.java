package sd2526.trab.client.grpc;

import java.io.IOException;
import java.net.URI;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import sd2526.trab.api.User;
import sd2526.trab.api.grpc.GrpcUsersGrpc;
import sd2526.trab.api.java.Users;
import sd2526.trab.server.grpc.utils.DataModelAdaptor;
import sd2526.trab.api.grpc.Messages.GrpcMessage;
import sd2526.trab.api.grpc.Users.DeleteUserArgs;
import sd2526.trab.api.grpc.Users.DeleteUserResult;
import sd2526.trab.api.grpc.Users.GetUserArgs;
import sd2526.trab.api.grpc.Users.GetUserResult;
import sd2526.trab.api.grpc.Users.GrpcUser;
import sd2526.trab.api.grpc.Users.GrpcUserOrBuilder;
import sd2526.trab.api.grpc.Users.PostUserResult;
import sd2526.trab.api.grpc.Users.SearchUsersArgs;
import sd2526.trab.api.grpc.Users.UpdateUserArgs;
import sd2526.trab.api.grpc.Users.UpdateUserResult;
import sd2526.trab.api.grpc.Users.GrpcUser.Builder;
import java.util.logging.Logger;
import sd2526.trab.api.java.*;
import sd2526.trab.api.java.Result;
import io.grpc.Status;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

public class GrpcUsersClient implements Users {
  private final GrpcUsersGrpc.GrpcUsersBlockingStub stub;

  public GrpcUsersClient(URI serverUri) {

    Channel channel = ManagedChannelBuilder.forAddress(serverUri.getHost(), serverUri.getPort())
        .enableRetry().usePlaintext().build();
    this.stub = GrpcUsersGrpc.newBlockingStub(channel);
  }

  @Override
  public Result<String> postUser(User user) {
    try {
      GrpcUser grpcUser = DataModelAdaptor.User_to_GrpcUser(user);
      PostUserResult res = stub.postUser(grpcUser);

      return Result.ok(res.getUserAddress());
    } catch (StatusRuntimeException sre) {
      return Result.error(statusToErrorCode(sre.getStatus()));
    }
  }

  @Override
  public Result<User> getUser(String name, String pwd) {
    try {
      // Cannot modify the message
      GetUserArgs args = GetUserArgs.newBuilder()
          .setName(pwd)
          .setPwd(pwd)
          .build();
      GetUserResult res = stub.getUser(args);
      return Result.ok(DataModelAdaptor.GrpcUser_to_User(res.getUser()));
    } catch (StatusRuntimeException sre) {
      return Result.error(statusToErrorCode(sre.getStatus()));
    }
  }

  @Override
  public Result<User> updateUser(String name, String pwd, User info) {
    try {
      GrpcUser new_info = DataModelAdaptor.User_to_GrpcUser(info);
      UpdateUserArgs update = UpdateUserArgs.newBuilder()
          .setName(name)
          .setPwd(pwd)
          .setInfo(new_info).build();
      UpdateUserResult res = stub.updateUser(update);
      return Result.ok(DataModelAdaptor.GrpcUser_to_User(res.getUser()));
    } catch (StatusRuntimeException sre) {
      return Result.error(statusToErrorCode(sre.getStatus()));
    }
  }

  @Override
  public Result<User> deleteUser(String name, String pwd) {
    try {
      DeleteUserArgs args = DeleteUserArgs.newBuilder()
          .setName(name)
          .setPwd(pwd)
          .build();
      DeleteUserResult res = stub.deleteUser(args);
      return Result.ok(DataModelAdaptor.GrpcUser_to_User(res.getUser()));
    } catch (StatusRuntimeException sre) {
      return Result.error(statusToErrorCode(sre.getStatus()));
    }
  }

  @Override
  public Result<List<User>> searchUsers(String name, String pwd, String query) {
    try {
      SearchUsersArgs args = SearchUsersArgs.newBuilder()
          .setName(name)
          .setPwd(pwd)
          .setQuery(query)
          .build();
      Iterator<GrpcUser> res = stub.searchUsers(args);

      List<User> ret = new ArrayList<User>();
      while (res.hasNext()) {
        ret.add(DataModelAdaptor.GrpcUser_to_User(res.next()));
      }
      return Result.ok(ret);
    } catch (StatusRuntimeException sre) {
      return Result.error(statusToErrorCode(sre.getStatus()));
    }
  }

  private Result.ErrorCode statusToErrorCode(Status status) {
    switch (status.getCode()) {
      case NOT_FOUND:
        return Result.ErrorCode.NOT_FOUND;
      case ALREADY_EXISTS:
        return Result.ErrorCode.CONFLICT;
      case PERMISSION_DENIED:
        return Result.ErrorCode.FORBIDDEN;
      case INVALID_ARGUMENT:
        return Result.ErrorCode.BAD_REQUEST;
      case UNIMPLEMENTED:
        return Result.ErrorCode.NOT_IMPLEMENTED;
      default:
        return Result.ErrorCode.INTERNAL_ERROR;
    }
  }
}
