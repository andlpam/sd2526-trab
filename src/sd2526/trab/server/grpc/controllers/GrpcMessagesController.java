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
import sd2526.trab.discovery.Discovery;
import io.grpc.Status;
import java.util.List;

public class GrpcMessagesController implements GrpcUsersGrpc.AsyncService, BindableService {
  private Hibernate hibernate;
  private final String domain;
  private static Logger Log = Logger.getLogger(GrpcMessagesController.class.getName());
  private Discovery discovery;

  public GrpcMessagesController(String domain, Discovery discovery) {
    this.domain = domain;
    this.discovery = discovery;
  }

  @Override
  public final ServerServiceDefinition bindService() {
    hibernate = Hibernate.getInstance();
    // Connect this instance to the server skeleton
    return GrpcUsersGrpc.bindService(this);
  }
}
