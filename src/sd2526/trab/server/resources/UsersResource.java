package sd2526.trab.server.resources;

import java.util.List;
import java.util.ArrayList;
import java.net.URI;
import java.util.logging.Logger;
import org.hibernate.exception.ConstraintViolationException;
import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;
import sd2526.trab.discovery.Discovery;
import sd2526.trab.api.User;
import sd2526.trab.api.rest.RestUsers;
import sd2526.trab.server.persistence.Hibernate;
import sd2526.trab.client.rest.RestMessagesClient;
import sd2526.trab.api.java.*;

@Singleton
public class UsersResource implements RestUsers {

  private final Hibernate hibernate;
  private final Discovery d;
  private final String domain;

  private static Logger Log = Logger.getLogger(UsersResource.class.getName());

  private RestMessagesClient localMessageClient = null;

  public UsersResource(Discovery discovery, String domain) {
    hibernate = Hibernate.getInstance();
    this.d = discovery;
    this.domain = domain;
  }

  private synchronized RestMessagesClient getLocalMessageClient() {
    if (localMessageClient != null) {
      return localMessageClient;
    }

    // First time searching for users client
    String serviceName = "Messages";
    try {
      Result<URI[]> uris = d.knownUrisOf(serviceName, this.domain, 1);

      if (!uris.isOK()) {
        // This might be wrong, fix later
        // Timeout if server not being able to answer 5 seconds later
        Log.info("Server is not being able to delivering uris by multicast.");
        throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);
      }

      localMessageClient = new RestMessagesClient(uris.value()[0]);
      return localMessageClient;

    } catch (WebApplicationException w) {
      throw w;
    } catch (Exception e) {
      // Unexpected error
      Log.severe("Erro inesperado a criar o MessageClient: " + e.getMessage());
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }

  }

  // I am not sure if I need to use syncronize here or not in all these methods
  /*
   * @Override
   * public String postUser(User user) {
   * // Log.info("postUser : " + user);
   * 
   * // Check if user data is valid
   * if (user.getName() == null || user.getPwd() == null || user.getDisplayName()
   * == null
   * || user.getDomain() == null) {
   * Log.info("User object invalid.");
   * throw new WebApplicationException(Status.BAD_REQUEST);
   * }
   * 
   * if (!user.getDomain().equals(this.domain)) {
   * Log.info("User domain does not match server domain.");
   * throw new WebApplicationException(Status.FORBIDDEN);
   * }
   * 
   * // Insert new user, checking if name already exists
   * try {
   * // Search the user in the data base
   * User existingUser = hibernate.get(User.class, user.getName());
   * 
   * if (existingUser != null) {
   * // Verifies if the data are exactly the same
   * if (existingUser.getPwd().equals(user.getPwd()) &&
   * existingUser.getDisplayName().equals(user.getDisplayName()) &&
   * existingUser.getDomain().equals(user.getDomain())) {
   * 
   * Log.info("User with the exact same data.");
   * return user.getName() + "@" + user.getDomain();
   * } else {
   * // CONFLICT
   * Log.info("User already exists.");
   * throw new WebApplicationException(Status.CONFLICT);
   * }
   * }
   * 
   * } catch (ConstraintViolationException e) {
   * e.printStackTrace();
   * Log.info("User already exists");
   * 
   * } catch (Exception x) {
   * x.printStackTrace();
   * throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
   * }
   * // If the user doesn't exist
   * hibernate.persist(user);
   * return user.getName() + "@" + user.getDomain();
   * }
   */

  @Override
  public String postUser(User user) {
    if (user.getName() == null || user.getPwd() == null || user.getDisplayName() == null
        || user.getDomain() == null) {
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    if (!user.getDomain().equals(this.domain)) {
      throw new WebApplicationException(Status.FORBIDDEN);
    }

    try {
      User existingUser = hibernate.get(User.class, user.getName());

      if (existingUser != null) {
        if (existingUser.getPwd().equals(user.getPwd()) &&
            existingUser.getDisplayName().equals(user.getDisplayName()) &&
            existingUser.getDomain().equals(user.getDomain())) {
          return user.getName() + "@" + user.getDomain();
        } else {
          throw new WebApplicationException(Status.CONFLICT);
        }
      }

      hibernate.persist(user);
      return user.getName() + "@" + user.getDomain();

    } catch (WebApplicationException w) {
      throw w;
    } catch (ConstraintViolationException e) {
      Log.info("User already exists");
      throw new WebApplicationException(Status.CONFLICT);
    } catch (Exception x) {
      x.printStackTrace();
      throw new WebApplicationException(Status.CONFLICT);
    }
  }

  @Override
  public User getUser(String name, String pwd) {
    // Log.info("getUser : name = " + name + "; pwd = " + pwd);

    // Check if parameters are valid
    if (name == null || pwd == null) {
      Log.info("Name or password null.");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    User user = null;

    try {
      user = hibernate.get(User.class, name);
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }

    // Check if user exists and password matches
    if (user == null || !user.getPwd().equals(pwd)) {
      Log.info("User does not exist or password is incorrect.");
      throw new WebApplicationException(Status.FORBIDDEN);
    }

    return user;
  }

  // Only update the user or create the user when he sends or receives a message
  @Override
  public User updateUser(String name, String pwd, User info) {
    // Log.info("updateUser : name = " + name + "; pwd = " + pwd + " ; info = " +
    // info);

    // Check if parameters are valid
    if (name == null || pwd == null || info == null) {
      Log.info("Name or password or info are null.");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    if (info.getName() != null && !info.getName().equals(name)) {
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    User user = null;
    try {
      user = hibernate.get(User.class, name);
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }
    if (user == null || !user.getPwd().equals(pwd))
      throw new WebApplicationException(Status.FORBIDDEN);

    // Update parameters
    if (info.getPwd() != null)
      user.setPwd(info.getPwd());
    if (info.getDisplayName() != null)
      user.setDisplayName(info.getDisplayName());

    try {
      hibernate.update(user);
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }

    return user;
  }

  // This method must be asyncronous and calls imediatly the server of the user
  @Override
  public User deleteUser(String name, String pwd) {
    // Log.info("deleteUser : name = " + name + "; pwd = " + pwd);
    if (name == null || pwd == null) {
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    // Search User
    User user;
    try {
      user = hibernate.get(User.class, name);
    } catch (Exception x) {
      x.printStackTrace();
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }

    // Security validation
    if (user == null || !user.getPwd().equals(pwd)) {
      throw new WebApplicationException(Status.FORBIDDEN);
    }

    // Delete data base
    try {
      hibernate.delete(user);
    } catch (Exception x) {
      x.printStackTrace();
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }

    // Lançar uma Thread para não bloquear a resposta ao cliente
    new Thread(() -> {
      try {
        Log.info("Warn MessagesServer to delete messages from user " + name + "...");
      } catch (Exception e) {
        Log.warning("Error deleting user messages: " + e.getMessage());
      }
    }).start();

    return user;
  }

  /*
   * @Override
   * public List<User> searchUsers(String name, String pwd, String pattern) {
   * // Log.info("searchUsers : name = " + name + "; pwd = " + pwd +
   * "; pattern = " +
   * // pattern);
   * 
   * if (name == null || pwd == null) {
   * throw new WebApplicationException(Status.BAD_REQUEST);
   * }
   * 
   * try {
   * // Get the user from the database
   * User caller = hibernate.get(User.class, name);
   * if (caller == null || !caller.getPwd().equals(pwd)) {
   * // If the user doesn't exist or the pwd is wrong
   * throw new WebApplicationException(Status.FORBIDDEN);
   * }
   * 
   * // Create the query
   * String query = "SELECT u FROM User u";
   * if (pattern != null && !pattern.isEmpty()) {
   * String p = pattern.toLowerCase();
   * 
   * query += " WHERE LOWER(u.name) LIKE '%" + p +
   * "%' OR LOWER(u.displayName) LIKE '%" + p + "%'";
   * }
   * 
   * List<User> hits = hibernate.jpql(query, User.class);
   * 
   * // Clean the passwords
   * for (User u : hits) {
   * u.setPwd("");
   * }
   * 
   * // Return the list with the clean passwords
   * return hits;
   * 
   * } catch (WebApplicationException w) {
   * throw w;
   * } catch (Exception x) {
   * x.printStackTrace();
   * throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
   * }
   * }
   */

  @Override
  public List<User> searchUsers(String name, String pwd, String pattern) {
    if (name == null || pwd == null) {
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    try {
      User caller = hibernate.get(User.class, name);
      if (caller == null || !caller.getPwd().equals(pwd)) {
        throw new WebApplicationException(Status.FORBIDDEN);
      }

      String query = "SELECT u FROM User u";
      if (pattern != null && !pattern.isEmpty()) {
        String p = pattern.toLowerCase();
        query += " WHERE LOWER(u.name) LIKE '%" + p + "%'";
      }

      List<User> hits = hibernate.jpql(query, User.class);

      List<User> cleanHits = new ArrayList<>();
      for (User u : hits) {
        cleanHits.add(new User(u.getName(), "", u.getDisplayName(), u.getDomain()));
      }

      return cleanHits;

    } catch (WebApplicationException w) {
      throw w;
    } catch (Exception x) {
      x.printStackTrace();
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }
  }

}
