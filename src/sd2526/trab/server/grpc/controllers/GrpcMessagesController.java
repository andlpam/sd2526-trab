package sd2526.trab.server.grpc.controllers;

import com.google.protobuf.Empty;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;
import sd2526.trab.api.grpc.GrpcMessagesGrpc;
import sd2526.trab.api.grpc.Messages.*;
import sd2526.trab.server.grpc.utils.DataModelAdaptor;
import sd2526.trab.server.java.JavaMessages;
import sd2526.trab.discovery.Discovery;

public class GrpcMessagesController extends GrpcController implements GrpcMessagesGrpc.AsyncService, BindableService {

  private final JavaMessages impl;

  public GrpcMessagesController(String serviceName, String domain, Discovery discovery) {
    this.impl = new JavaMessages(serviceName, domain, discovery);
  }

  @Override
  public final ServerServiceDefinition bindService() {
    return GrpcMessagesGrpc.bindService(this);
  }

  @Override
  public void postMessage(PostMessageArgs request, StreamObserver<PostMessageResult> responseObserver) {
    super.toGrpcResult(responseObserver,
        impl.postMessage(request.getPwd(), DataModelAdaptor.GrpcMessage_to_Message(request.getMessage())),
        (mid) -> PostMessageResult.newBuilder().setMid(mid).build());
  }

  @Override
  public void getInboxMessage(GetInboxMessageArgs request, StreamObserver<GrpcMessage> responseObserver) {
    super.toGrpcResult(responseObserver,
        impl.getInboxMessage(request.getName(), request.getMid(), request.getPwd()),
        DataModelAdaptor::Message_to_GrpcMessage);
  }

  @Override
  public void getAllInboxMessages(GetAllInboxMessagesArgs request,
      StreamObserver<GetAllInboxMessagesResult> responseObserver) {
    super.toGrpcResult(responseObserver,
        impl.getAllInboxMessages(request.getName(), request.getPwd()),
        (mids) -> GetAllInboxMessagesResult.newBuilder().addAllMids(mids).build());
  }

  @Override
  public void searchInbox(SearchInboxArgs request, StreamObserver<SearchInboxResult> responseObserver) {
    super.toGrpcResult(responseObserver,
        impl.searchInbox(request.getName(), request.getPwd(), request.getQuery()),
        (mids) -> SearchInboxResult.newBuilder().addAllMids(mids).build());
  }

  @Override
  public void removeInboxMessage(RemoveInboxMessageArgs request, StreamObserver<Empty> responseObserver) {
    super.toGrpcResult(responseObserver,
        impl.removeInboxMessage(request.getName(), request.getMid(), request.getPwd()),
        (v) -> Empty.newBuilder().build());
  }

  @Override
  public void deleteMessage(DeleteMessageArgs request, StreamObserver<Empty> responseObserver) {
    super.toGrpcResult(responseObserver,
        impl.deleteMessage(request.getName(), request.getMid(), request.getPwd()),
        (v) -> Empty.newBuilder().build());
  }
}