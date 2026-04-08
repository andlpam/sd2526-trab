package sd2526.trab.client.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import sd2526.trab.api.Message;
import sd2526.trab.api.grpc.GrpcMessagesGrpc;
import sd2526.trab.api.grpc.Messages.*;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.server.grpc.utils.DataModelAdaptor;
import java.net.URI;
import java.util.List;

public class GrpcMessagesClient extends GrpcClient implements Messages {

  final ManagedChannel channel;
  final GrpcMessagesGrpc.GrpcMessagesBlockingStub stub;

  public GrpcMessagesClient(URI serverUri) {
    this.channel = ManagedChannelBuilder.forAddress(serverUri.getHost(), serverUri.getPort())
        .usePlaintext()
        .build();
    this.stub = GrpcMessagesGrpc.newBlockingStub(channel);
  }

  @Override
  public Result<String> postMessage(String pwd, Message msg) {
    return super.reTry(() -> {
      PostMessageArgs req = PostMessageArgs.newBuilder().setPwd(pwd)
          .setMessage(DataModelAdaptor.Message_to_GrpcMessage(msg)).build();
      return Result.ok(stub.postMessage(req).getMid());
    });
  }

  @Override
  public Result<Message> getInboxMessage(String name, String mid, String pwd) {
    return super.reTry(() -> {
      GetInboxMessageArgs req = GetInboxMessageArgs.newBuilder()
          .setName(name).setMid(mid).setPwd(pwd).build();
      return Result.ok(DataModelAdaptor.GrpcMessage_to_Message(stub.getInboxMessage(req)));
    });
  }

  @Override
  public Result<List<String>> getAllInboxMessages(String name, String pwd) {
    return super.reTry(() -> {
      GetAllInboxMessagesArgs req = GetAllInboxMessagesArgs.newBuilder()
          .setName(name).setPwd(pwd).build();
      return Result.ok(stub.getAllInboxMessages(req).getMidsList());
    });
  }

  @Override
  public Result<List<String>> searchInbox(String name, String pwd, String query) {
    return super.reTry(() -> {
      SearchInboxArgs req = SearchInboxArgs.newBuilder()
          .setName(name).setPwd(pwd).setQuery(query).build();
      return Result.ok(stub.searchInbox(req).getMidsList());
    });
  }

  @Override
  public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
    return super.reTry(() -> {
      RemoveInboxMessageArgs req = RemoveInboxMessageArgs.newBuilder()
          .setName(name).setMid(mid).setPwd(pwd).build();
      stub.removeInboxMessage(req);
      return Result.ok();
    });
  }

  @Override
  public Result<Void> deleteMessage(String name, String mid, String pwd) {
    return super.reTry(() -> {
      DeleteMessageArgs req = DeleteMessageArgs.newBuilder()
          .setName(name).setMid(mid).setPwd(pwd).build();
      stub.deleteMessage(req);
      return Result.ok();
    });
  }
}