package sd2526.trab.server.rest.resources;

import sd2526.trab.api.Message;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.discovery.Discovery;
import sd2526.trab.server.java.JavaMessages;
import java.util.List;

public class MessagesResource extends RestResource implements RestMessages {

  private final JavaMessages impl;

  public MessagesResource(String serviceName, String domain, Discovery discovery) {
    this.impl = new JavaMessages(serviceName, domain, discovery);
  }

  @Override
  public String postMessage(String pwd, Message msg) {
    return super.unwrapResultOrThrow(impl.postMessage(pwd, msg));
  }

  @Override
  public Message getMessage(String name, String mid, String pwd) {
    return super.unwrapResultOrThrow(impl.getInboxMessage(name, mid, pwd));
  }

  @Override
  public List<String> getMessages(String name, String pwd, String query) {
    return super.unwrapResultOrThrow(impl.searchInbox(name, pwd, query));
  }

  @Override
  public void removeFromUserInbox(String name, String mid, String pwd) {
    super.unwrapResultOrThrow(impl.removeInboxMessage(name, mid, pwd));
  }

  @Override
  public void deleteMessage(String name, String mid, String pwd) {
    super.unwrapResultOrThrow(impl.deleteMessage(name, mid, pwd));
  }
}