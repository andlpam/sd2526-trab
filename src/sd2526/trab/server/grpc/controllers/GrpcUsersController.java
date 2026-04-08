package sd2526.trab.server.grpc.controllers;

import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;
import sd2526.trab.api.grpc.GrpcUsersGrpc;
import sd2526.trab.api.grpc.Users.*;
import sd2526.trab.server.grpc.utils.DataModelAdaptor;
import sd2526.trab.server.java.JavaUsers;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Result;
import java.util.List;

public class GrpcUsersController extends GrpcController implements GrpcUsersGrpc.AsyncService, BindableService {

  private final JavaUsers impl;

  public GrpcUsersController(String domain) {
    this.impl = new JavaUsers(domain);
  }

  @Override
  public final ServerServiceDefinition bindService() {
    return GrpcUsersGrpc.bindService(this);
  }

  @Override
  public void postUser(GrpcUser grpcUser, StreamObserver<PostUserResult> responseObserver) {
    super.toGrpcResult(responseObserver, impl.postUser(DataModelAdaptor.GrpcUser_to_User(grpcUser)),
        (userAddress) -> PostUserResult.newBuilder().setUserAddress(userAddress).build());
  }

  @Override
  public void getUser(GetUserArgs request, StreamObserver<GetUserResult> responseObserver) {
    super.toGrpcResult(responseObserver, impl.getUser(request.getName(), request.getPwd()),
        (user) -> GetUserResult.newBuilder().setUser(DataModelAdaptor.User_to_GrpcUser(user)).build());
  }

  @Override
  public void updateUser(UpdateUserArgs request, StreamObserver<UpdateUserResult> responseObserver) {
    super.toGrpcResult(responseObserver,
        impl.updateUser(request.getName(), request.getPwd(), DataModelAdaptor.GrpcUser_to_User(request.getInfo())),
        (user) -> UpdateUserResult.newBuilder().setUser(DataModelAdaptor.User_to_GrpcUser(user)).build());
  }

  @Override
  public void deleteUser(DeleteUserArgs request, StreamObserver<DeleteUserResult> responseObserver) {
    super.toGrpcResult(responseObserver,
        impl.deleteUser(request.getName(), request.getPwd()),
        (user) -> DeleteUserResult.newBuilder().setUser(DataModelAdaptor.User_to_GrpcUser(user)).build());
  }

  @Override
  public void searchUsers(SearchUsersArgs request, StreamObserver<GrpcUser> responseObserver) {

    Result<List<User>> result = impl.searchUsers(request.getName(), request.getPwd(), request.getQuery());

    if (result.isOK()) {
      for (User u : result.value()) {
        responseObserver.onNext(DataModelAdaptor.User_to_GrpcUser(u));
      }
      responseObserver.onCompleted();
    } else {
      responseObserver.onError(errorCodeToStatus(result.error()));
    }
  }
}