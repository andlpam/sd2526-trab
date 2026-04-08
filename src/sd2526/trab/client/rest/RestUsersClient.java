package sd2526.trab.client.rest;

import java.net.URI;
import java.util.List;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.GenericType;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Users;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.rest.RestUsers;

public class RestUsersClient extends RestClient implements Users {

  public RestUsersClient(URI serverUri) {
    super(serverUri);
    this.target = super.target.path(RestUsers.PATH);
  }

  @Override
  public Result<String> postUser(User user) {
    return super.reTry(() -> {
      Response r = target.request()
          .accept(MediaType.APPLICATION_JSON)
          .post(Entity.entity(user, MediaType.APPLICATION_JSON));
      return super.processResponse(r, String.class);
    });
  }

  @Override
  public Result<User> getUser(String name, String pwd) {
    return super.reTry(() -> {
      Response r = target.path(name).queryParam(RestUsers.PWD, pwd)
          .request().accept(MediaType.APPLICATION_JSON).get();
      return super.processResponse(r, User.class);
    });
  }

  @Override
  public Result<User> updateUser(String name, String pwd, User info) {
    return super.reTry(() -> {
      Response r = target.path(name).queryParam(RestUsers.PWD, pwd)
          .request().accept(MediaType.APPLICATION_JSON)
          .put(Entity.entity(info, MediaType.APPLICATION_JSON));
      return super.processResponse(r, User.class);
    });
  }

  @Override
  public Result<User> deleteUser(String name, String pwd) {
    return super.reTry(() -> {
      Response r = target.path(name).queryParam(RestUsers.PWD, pwd)
          .request().accept(MediaType.APPLICATION_JSON).delete();
      return super.processResponse(r, User.class);
    });
  }

  @Override
  public Result<List<User>> searchUsers(String name, String pwd, String pattern) {
    return super.reTry(() -> {
      Response r = target.queryParam(RestUsers.NAME, name)
          .queryParam(RestUsers.PWD, pwd)
          .queryParam(RestUsers.QUERY, pattern)
          .request().accept(MediaType.APPLICATION_JSON).get();

      return super.processResponse(r, new GenericType<List<User>>() {
      });
    });
  }
}