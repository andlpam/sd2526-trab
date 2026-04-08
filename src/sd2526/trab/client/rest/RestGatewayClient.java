package sd2526.trab.client.rest;

import java.net.URI;
import java.util.List;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import sd2526.trab.api.*;
import sd2526.trab.api.java.*;
import sd2526.trab.api.rest.RestGateway;

public class RestGatewayClient extends RestClient implements Users, Messages {

  public RestGatewayClient(URI serverUri) {
    super(serverUri);

    this.target = super.target.path(RestGateway.PATH);
  }

  @Override
  public Result<String> postUser(User user) {
    return super.reTry(() -> {
      Response r = target.path("users").request().accept(MediaType.APPLICATION_JSON)
          .post(Entity.entity(user, MediaType.APPLICATION_JSON));
      return super.processResponse(r, String.class);
    });
  }

  @Override
  public Result<User> getUser(String name, String pwd) {
    return super.reTry(() -> {
      Response r = target.path("users").path(name).queryParam("pwd", pwd)
          .request().accept(MediaType.APPLICATION_JSON).get();
      return super.processResponse(r, User.class);
    });
  }

  @Override
  public Result<User> updateUser(String name, String pwd, User info) {
    return super.reTry(() -> {
      Response r = target.path("users").path(name).queryParam("pwd", pwd)
          .request().accept(MediaType.APPLICATION_JSON)
          .put(Entity.entity(info, MediaType.APPLICATION_JSON));
      return super.processResponse(r, User.class);
    });
  }

  @Override
  public Result<User> deleteUser(String name, String pwd) {
    return super.reTry(() -> {
      Response r = target.path("users").path(name).queryParam("pwd", pwd)
          .request().accept(MediaType.APPLICATION_JSON).delete();
      return super.processResponse(r, User.class);
    });
  }

  @Override
  public Result<List<User>> searchUsers(String name, String pwd, String pattern) {
    return super.reTry(() -> {
      Response r = target.path("users").queryParam("name", name).queryParam("pwd", pwd).queryParam("query", pattern)
          .request().accept(MediaType.APPLICATION_JSON).get();
      return super.processResponse(r, new GenericType<List<User>>() {
      });
    });
  }

  @Override
  public Result<String> postMessage(String pwd, Message msg) {
    return super.reTry(() -> {
      Response r = target.path("messages").queryParam("pwd", pwd)
          .request().accept(MediaType.APPLICATION_JSON)
          .post(Entity.entity(msg, MediaType.APPLICATION_JSON));
      return super.processResponse(r, String.class);
    });
  }

  @Override
  public Result<Message> getInboxMessage(String name, String mid, String pwd) {
    return super.reTry(() -> {
      Response r = target.path("messages").path("mbox").path(name).path(mid).queryParam("pwd", pwd)
          .request().accept(MediaType.APPLICATION_JSON).get();
      return super.processResponse(r, Message.class);
    });
  }

  @Override
  public Result<List<String>> getAllInboxMessages(String name, String pwd) {
    return searchInbox(name, pwd, "");
  }

  @Override
  public Result<List<String>> searchInbox(String name, String pwd, String query) {
    return super.reTry(() -> {
      Response r = target.path("messages").path("mbox").path(name).queryParam("pwd", pwd).queryParam("query", query)
          .request().accept(MediaType.APPLICATION_JSON).get();
      return super.processResponse(r, new GenericType<List<String>>() {
      });
    });
  }

  @Override
  public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
    return super.reTry(() -> {
      Response r = target.path("messages").path("mbox").path(name).path(mid).queryParam("pwd", pwd)
          .request().delete();
      return super.processResponse(r, Void.class);
    });
  }

  @Override
  public Result<Void> deleteMessage(String name, String mid, String pwd) {
    return super.reTry(() -> {
      Response r = target.path("messages").path(name).path(mid).queryParam("pwd", pwd)
          .request().delete();
      return super.processResponse(r, Void.class);
    });
  }
}