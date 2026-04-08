package sd2526.trab.server.rest.resources;

import sd2526.trab.api.User;
import sd2526.trab.api.rest.RestUsers;
import sd2526.trab.server.java.JavaUsers;
import jakarta.inject.Singleton;
import java.util.List;

@Singleton
public class UsersResource extends RestResource implements RestUsers {

  final JavaUsers impl;

  public UsersResource(String domain) {
    this.impl = new JavaUsers(domain);
  }

  @Override
  public String postUser(User user) {
    return super.unwrapResultOrThrow(impl.postUser(user));
  }

  @Override
  public User getUser(String name, String pwd) {
    return super.unwrapResultOrThrow(impl.getUser(name, pwd));
  }

  @Override
  public User updateUser(String name, String pwd, User info) {
    return super.unwrapResultOrThrow(impl.updateUser(name, pwd, info));
  }

  @Override
  public User deleteUser(String name, String pwd) {
    return super.unwrapResultOrThrow(impl.deleteUser(name, pwd));
  }

  @Override
  public List<User> searchUsers(String name, String pwd, String pattern) {
    return super.unwrapResultOrThrow(impl.searchUsers(name, pwd, pattern));
  }
}