package sd2526.trab.server.java;

import sd2526.trab.api.User;
import sd2526.trab.api.java.Users;
import sd2526.trab.api.java.Result;
import sd2526.trab.server.persistence.Hibernate;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Logger;

public class JavaUsers implements Users {
  private static Logger Log = Logger.getLogger(JavaUsers.class.getName());
  private final Hibernate hibernate;
  private final String domain;

  public JavaUsers(String domain) {
    this.hibernate = Hibernate.getInstance();
    this.domain = domain;
  }

  @Override
  public Result<String> postUser(User user) {
    if (user.getName() == null || user.getPwd() == null || user.getDisplayName() == null || user.getDomain() == null) {
      return Result.error(Result.ErrorCode.BAD_REQUEST);
    }
    if (!user.getDomain().equals(this.domain)) {
      return Result.error(Result.ErrorCode.FORBIDDEN);
    }


    synchronized (user.getName().intern()) {
      try {
        User existingUser = hibernate.get(User.class, user.getName());
        if (existingUser != null) {
          if (existingUser.getPwd().equals(user.getPwd()) &&
              existingUser.getDisplayName().equals(user.getDisplayName()) &&
              existingUser.getDomain().equals(user.getDomain())) {
            return Result.ok(user.getName() + "@" + user.getDomain());
          } else {
            return Result.error(Result.ErrorCode.CONFLICT);
          }
        }
        hibernate.persist(user);
        return Result.ok(user.getName() + "@" + user.getDomain());
      } catch (org.hibernate.exception.ConstraintViolationException e) {
        return Result.error(Result.ErrorCode.CONFLICT);
      } catch (Exception e) {
        e.printStackTrace();
        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
      }
    }
  }

  @Override
  public Result<User> getUser(String name, String pwd) {
    if (name == null || pwd == null)
      return Result.error(Result.ErrorCode.BAD_REQUEST);
    try {
      User user = hibernate.get(User.class, name);
      if (user == null || !user.getPwd().equals(pwd)) {
        return Result.error(Result.ErrorCode.FORBIDDEN);
      }
      return Result.ok(user);
    } catch (Exception e) {
      return Result.error(Result.ErrorCode.INTERNAL_ERROR);
    }
  }

  @Override
  public Result<User> updateUser(String name, String pwd, User info) {
    if (name == null || pwd == null || info == null)
      return Result.error(Result.ErrorCode.BAD_REQUEST);
    if (info.getName() != null && !info.getName().equals(name))
      return Result.error(Result.ErrorCode.BAD_REQUEST);

    synchronized (name.intern()) {
      try {
        User user = hibernate.get(User.class, name);
        if (user == null || !user.getPwd().equals(pwd))
          return Result.error(Result.ErrorCode.FORBIDDEN);

        if (info.getPwd() != null)
          user.setPwd(info.getPwd());
        if (info.getDisplayName() != null)
          user.setDisplayName(info.getDisplayName());

        hibernate.update(user);
        return Result.ok(user);
      } catch (Exception e) {
        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
      }
    }
  }

  @Override
  public Result<User> deleteUser(String name, String pwd) {
    if (name == null || pwd == null)
      return Result.error(Result.ErrorCode.BAD_REQUEST);

    synchronized (name.intern()) {
      try {
        User user = hibernate.get(User.class, name);
        if (user == null || !user.getPwd().equals(pwd))
          return Result.error(Result.ErrorCode.FORBIDDEN);

        hibernate.delete(user);

        return Result.ok(user);
      } catch (Exception e) {
        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
      }
    }
  }

  @Override
  public Result<List<User>> searchUsers(String name, String pwd, String pattern) {
    if (name == null || pwd == null)
      return Result.error(Result.ErrorCode.BAD_REQUEST);
    try {
      User caller = hibernate.get(User.class, name);
      if (caller == null || !caller.getPwd().equals(pwd))
        return Result.error(Result.ErrorCode.FORBIDDEN);

      String query = "SELECT u FROM User u";
      if (pattern != null && !pattern.isEmpty()) {
        query += " WHERE LOWER(u.name) LIKE '%" + pattern.toLowerCase() + "%'";
      }

      List<User> hits = hibernate.jpql(query, User.class);
      List<User> cleanHits = new ArrayList<>();
      for (User u : hits)
        cleanHits.add(new User(u.getName(), "", u.getDisplayName(), u.getDomain()));

      return Result.ok(cleanHits);
    } catch (Exception e) {
      return Result.error(Result.ErrorCode.INTERNAL_ERROR);
    }
  }
}