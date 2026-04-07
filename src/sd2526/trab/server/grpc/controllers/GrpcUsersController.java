package sd2526.trab.server.grpc.controllers;

import sd2526.trab.api.User;
import sd2526.trab.api.grpc.GrpcUsersGrpc;
import sd2526.trab.api.grpc.Users.DeleteUserArgs;
import sd2526.trab.api.grpc.Users.DeleteUserResult;
import sd2526.trab.api.grpc.Users.GetUserArgs;
import sd2526.trab.api.grpc.Users.GetUserResult;
import sd2526.trab.api.grpc.Users.GrpcUser;
import sd2526.trab.api.grpc.Users.PostUserResult;
import sd2526.trab.api.grpc.Users.SearchUsersArgs;
import sd2526.trab.api.grpc.Users.UpdateUserArgs;
import sd2526.trab.api.grpc.Users.UpdateUserResult;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;
import sd2526.trab.server.grpc.utils.DataModelAdaptor;
import sd2526.trab.server.persistence.Hibernate;
import java.util.logging.Logger;
import sd2526.trab.api.java.*;
import sd2526.trab.api.java.Result;
import io.grpc.Status;
import java.util.List;

public class GrpcUsersController implements GrpcUsersGrpc.AsyncService, BindableService {
  private static Logger Log = Logger.getLogger(GrpcUsersController.class.getName());
  private Hibernate hibernate;
  private final String domain;

  @Override
  public final ServerServiceDefinition bindService() {
    hibernate = Hibernate.getInstance();
    // Connect this instance to the server skeleton
    return GrpcUsersGrpc.bindService(this);
  }

  public GrpcUsersController(String domain) {
    this.domain = domain;
  }

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

  @Override
  public void postUser(GrpcUser grpcUser, StreamObserver<PostUserResult> responseObserver) {
    Log.info("postUser : " + grpcUser.getName());

    if (grpcUser.getName().isEmpty() || !grpcUser.hasPwd() || !grpcUser.hasDisplayName() || !grpcUser.hasDomain()) {
      responseObserver.onError(errorCodeToStatus(Result.ErrorCode.BAD_REQUEST));
      return;
    }

    if (!grpcUser.getDomain().equals(this.domain)) {
      responseObserver.onError(errorCodeToStatus(Result.ErrorCode.FORBIDDEN));
      return;
    }

    try {
      User user = DataModelAdaptor.GrpcUser_to_User(grpcUser);
      User existingUser = hibernate.get(User.class, user.getName());

      if (existingUser != null) {
        if (existingUser.getPwd().equals(user.getPwd()) &&
            existingUser.getDisplayName().equals(user.getDisplayName()) &&
            existingUser.getDomain().equals(user.getDomain())) {
          responseObserver.onNext(PostUserResult.newBuilder()
              .setUserAddress(user.getName() + "@" + user.getDomain()).build());
          responseObserver.onCompleted();
          return;
        } else {
          responseObserver.onError(errorCodeToStatus(Result.ErrorCode.CONFLICT));
          return;
        }
      }

      hibernate.persist(user);
      responseObserver
          .onNext(PostUserResult.newBuilder().setUserAddress(user.getName() + "@" + user.getDomain()).build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      e.printStackTrace();
      responseObserver.onError(errorCodeToStatus(Result.ErrorCode.CONFLICT));
    }
  }

  @Override
  public void getUser(GetUserArgs request, StreamObserver<GetUserResult> responseObserver) {
    String name = request.getName();
    String pwd = request.getPwd();

    if (name.isEmpty()) {
      responseObserver.onError(errorCodeToStatus(Result.ErrorCode.BAD_REQUEST));
      return;
    }

    try {
      User user = hibernate.get(User.class, name);

      if (user == null || !user.getPwd().equals(pwd)) {
        responseObserver.onError(errorCodeToStatus(Result.ErrorCode.FORBIDDEN));
        return;
      }

      // Converte para gRPC e envia
      GrpcUser replyUser = DataModelAdaptor.User_to_GrpcUser(user);
      responseObserver.onNext(GetUserResult.newBuilder().setUser(replyUser).build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      e.printStackTrace();
      responseObserver.onError(errorCodeToStatus(Result.ErrorCode.INTERNAL_ERROR));
    }
  }

  @Override
  public void updateUser(UpdateUserArgs request, StreamObserver<UpdateUserResult> responseObserver) {
    String name = request.getName();
    String pwd = request.getPwd();
    GrpcUser info = request.getInfo();

    if (name.isEmpty() || !request.hasInfo()) {
      responseObserver.onError(errorCodeToStatus(Result.ErrorCode.BAD_REQUEST));
      return;
    }

    if (!info.getName().isEmpty() && !info.getName().equals(name)) {
      responseObserver.onError(errorCodeToStatus(Result.ErrorCode.BAD_REQUEST));
      return;
    }

    try {
      User user = hibernate.get(User.class, name);

      if (user == null || !user.getPwd().equals(pwd)) {
        responseObserver.onError(errorCodeToStatus(Result.ErrorCode.FORBIDDEN));
        return;
      }

      // Update parameters
      if (info.hasPwd())
        user.setPwd(info.getPwd());
      if (info.hasDisplayName())
        user.setDisplayName(info.getDisplayName());

      hibernate.update(user);

      // Devolver o utilizador atualizado
      GrpcUser replyUser = DataModelAdaptor.User_to_GrpcUser(user);
      responseObserver.onNext(UpdateUserResult.newBuilder().setUser(replyUser).build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      e.printStackTrace();
      responseObserver.onError(errorCodeToStatus(Result.ErrorCode.INTERNAL_ERROR));
    }
  }

  @Override
  public void deleteUser(DeleteUserArgs request, StreamObserver<DeleteUserResult> responseObserver) {
    String name = request.getName();
    String pwd = request.getPwd();

    if (name.isEmpty()) {
      responseObserver.onError(errorCodeToStatus(Result.ErrorCode.BAD_REQUEST));
      return;
    }

    try {
      User user = hibernate.get(User.class, name);

      if (user == null || !user.getPwd().equals(pwd)) {
        responseObserver.onError(errorCodeToStatus(Result.ErrorCode.FORBIDDEN));
        return;
      }

      hibernate.delete(user);

      // Lançar uma Thread para apagar as mensagens (igual ao REST)
      new Thread(() -> {
        try {
          Log.info("Warn MessagesServer to delete messages from user " + name + "...");
          // Aqui no futuro vais fazer a chamada ao cliente gRPC das mensagens
        } catch (Exception e) {
          Log.warning("Error deleting user messages: " + e.getMessage());
        }
      }).start();

      GrpcUser replyUser = DataModelAdaptor.User_to_GrpcUser(user);
      responseObserver.onNext(DeleteUserResult.newBuilder().setUser(replyUser).build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      e.printStackTrace();
      responseObserver.onError(errorCodeToStatus(Result.ErrorCode.INTERNAL_ERROR));
    }
  }

  @Override
  public void searchUsers(SearchUsersArgs request, StreamObserver<GrpcUser> responseObserver) {
    String name = request.getName();
    String pwd = request.getPwd();
    String pattern = request.getQuery();

    if (name.isEmpty()) {
      responseObserver.onError(errorCodeToStatus(Result.ErrorCode.BAD_REQUEST));
      return;
    }

    try {
      User caller = hibernate.get(User.class, name);
      if (caller == null || !caller.getPwd().equals(pwd)) {
        responseObserver.onError(errorCodeToStatus(Result.ErrorCode.FORBIDDEN));
        return;
      }

      String query = "SELECT u FROM User u";
      if (!pattern.isEmpty()) {
        String p = pattern.toLowerCase();
        query += " WHERE LOWER(u.name) LIKE '%" + p + "%'";
      }

      List<User> hits = hibernate.jpql(query, User.class);

      for (User u : hits) {

        User cleanUser = new User(u.getName(), "", u.getDisplayName(), u.getDomain());
        GrpcUser replyUser = DataModelAdaptor.User_to_GrpcUser(cleanUser);

        responseObserver.onNext(replyUser);
      }

      responseObserver.onCompleted();

    } catch (Exception e) {
      e.printStackTrace();
      responseObserver.onError(errorCodeToStatus(Result.ErrorCode.INTERNAL_ERROR));
    }
  }

}
