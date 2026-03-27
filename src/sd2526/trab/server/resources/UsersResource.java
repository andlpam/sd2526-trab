package sd2526.trab.server.resources;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;
import sd2526.trab.api.User;
import sd2526.trab.api.rest.RestUsers;

@Singleton
public class UsersResource implements RestUsers {

  private final Map<String, User> users;

  private static Logger Log = Logger.getLogger(UsersResource.class.getName());

  public UsersResource() {
    this.users = new ConcurrentHashMap<String, User>();
  }

  // I am not sure if I need to use syncronize here or not in all these methods
  @Override
  public String postUser(User user) {
    Log.info("postUser : " + user);

    // Check if user data is valid
    if (user.getName() == null || user.getPwd() == null || user.getDisplayName() == null
        || user.getDomain() == null) {
      Log.info("User object invalid.");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    // Insert new user, checking if name already exists
    User existing = users.putIfAbsent(user.getName(), user);
    if (existing != null) {
      // Same user (same pwd and displayName) is considered a success
      if (existing.getPwd().equals(user.getPwd()) && existing.getDisplayName().equals(user.getDisplayName())) {
        return existing.getName() + "@" + existing.getDomain();
      }
      Log.info("User already exists with different data.");
      throw new WebApplicationException(Status.CONFLICT);
    }

    return user.getName() + "@" + user.getDomain();
  }

  @Override
  public User getUser(String name, String pwd) {
    Log.info("getUser : name = " + name + "; pwd = " + pwd);

    // Check if parameters are valid
    if (name == null || pwd == null) {
      Log.info("Name or password null.");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    var user = users.get(name);

    // Check if user exists and password matches
    if (user == null || !user.getPwd().equals(pwd)) {
      Log.info("User does not exist or password is incorrect.");
      throw new WebApplicationException(Status.FORBIDDEN);
    }

    return user;
  }

  @Override
  public User updateUser(String name, String pwd, User info) {
    Log.info("updateUser : name = " + name + "; pwd = " + pwd + " ; info = " + info);

    var user = users.get(name);
    if (user == null)
      throw new WebApplicationException(Status.NOT_FOUND);
    if (!user.getPwd().equals(pwd))
      throw new WebApplicationException(Status.FORBIDDEN);

    // Atualizar os campos permitidos
    if (info.getPwd() != null)
      user.setPwd(info.getPwd());
    if (info.getDisplayName() != null)
      user.setDisplayName(info.getDisplayName());
    if (info.getDomain() != null)
      user.setDomain(info.getDomain());

    return user;
  }

  // This method must be asyncronous
  @Override
  public User deleteUser(String name, String pwd) {
    Log.info("deleteUser : name = " + name + "; pwd = " + pwd);

    var user = users.get(name);
    if (user == null)
      throw new WebApplicationException(Status.NOT_FOUND);
    if (!user.getPwd().equals(pwd))
      throw new WebApplicationException(Status.FORBIDDEN);

    return users.remove(name);
  }

  @Override
  public List<User> searchUsers(String name, String pwd, String pattern) {
    Log.info("searchUsers : name = " + name + "; pwd = " + pwd + "; pattern = " + pattern);
    return users.values().stream()
        .filter(u -> pattern == null || u.getName().toLowerCase().contains(pattern.toLowerCase()))
        .collect(Collectors.toList());
  }

}
