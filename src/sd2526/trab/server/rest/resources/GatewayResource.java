package sd2526.trab.server.rest.resources;

import sd2526.trab.client.ClientFactory;
import sd2526.trab.client.grpc.GrpcMessagesClient;
import sd2526.trab.client.grpc.GrpcUsersClient;
import sd2526.trab.client.rest.RestMessagesClient;
import sd2526.trab.client.rest.RestUsersClient;
import sd2526.trab.discovery.Discovery;
import sd2526.trab.api.*;
import sd2526.trab.api.java.*;
import sd2526.trab.api.rest.RestGateway;
import java.util.List;

public class GatewayResource extends RestResource implements RestGateway {

  private final String domain;
  private final Discovery discovery;

  private final ClientFactory<Users> usersFactory;
  private final ClientFactory<Messages> messagesFactory;

  public GatewayResource(String serviceName, String domain, Discovery discovery) {
    this.domain = domain;
    this.discovery = discovery;

    this.usersFactory = new ClientFactory<>(Users.SERVICE_NAME, RestUsersClient::new, GrpcUsersClient::new);
    this.messagesFactory = new ClientFactory<>(Messages.SERVICE_NAME, RestMessagesClient::new, GrpcMessagesClient::new);
  }

  @Override
  public String postUser(User user) {
    return super.unwrapResultOrThrow(usersFactory.get(discovery, domain).postUser(user));
  }

  @Override
  public User getUser(String name, String pwd) {
    return super.unwrapResultOrThrow(usersFactory.get(discovery, domain).getUser(name, pwd));
  }

  @Override
  public User updateUser(String name, String pwd, User info) {
    return super.unwrapResultOrThrow(usersFactory.get(discovery, domain).updateUser(name, pwd, info));
  }

  @Override
  public User deleteUser(String name, String pwd) {
    return super.unwrapResultOrThrow(usersFactory.get(discovery, domain).deleteUser(name, pwd));
  }

  @Override
  public List<User> searchUsers(String name, String pwd, String pattern) {
    return super.unwrapResultOrThrow(usersFactory.get(discovery, domain).searchUsers(name, pwd, pattern));
  }

  @Override
  public String postMessage(String pwd, Message msg) {
    return super.unwrapResultOrThrow(messagesFactory.get(discovery, domain).postMessage(pwd, msg));
  }

  @Override
  public Message getMessage(String name, String mid, String pwd) {
    return super.unwrapResultOrThrow(messagesFactory.get(discovery, domain).getInboxMessage(name, mid, pwd));
  }

  @Override
  public List<String> getMessages(String name, String pwd, String query) {
    return super.unwrapResultOrThrow(messagesFactory.get(discovery, domain).searchInbox(name, pwd, query));
  }

  @Override
  public void removeFromUserInbox(String name, String mid, String pwd) {
    super.unwrapResultOrThrow(messagesFactory.get(discovery, domain).removeInboxMessage(name, mid, pwd));
  }

  @Override
  public void deleteMessage(String name, String mid, String pwd) {
    super.unwrapResultOrThrow(messagesFactory.get(discovery, domain).deleteMessage(name, mid, pwd));
  }
}