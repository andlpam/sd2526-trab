package sd2526.trab.server.resources;

import sd2526.*;
import sd2526.trab.client.rest.RestMessagesClient;
import sd2526.trab.client.rest.RestUsersClient;
import sd2526.trab.discovery.Discovery;
import sd2526.trab.api.java.*;
import sd2526.trab.api.rest.RestMessages;
import java.util.UUID;
import java.util.logging.Logger;
import sd2526.trab.api.*;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MessagesResource implements RestMessages {
  private final Map<String, Map<String, Message>> mailBox;
  private final Discovery d;
  private final String domain;
  private final String serviceName;
  private static Logger Log = Logger.getLogger(MessagesResource.class.getName());

  // Can only contain one @
  private static final String REGEX_LOCAL = "^[^@<>\\s]+@[^@<>\\s]+$";

  private static final String REGEX_FORWARDING = "^.+\\s<[^@<>\\s]+@[^@<>\\s]+>$";

  private RestUsersClient localUsersClient = null;

  public MessagesResource(String serviceName, String domain, Discovery discovery) {
    this.mailBox = new ConcurrentHashMap<>();
    this.d = discovery;
    this.domain = domain;
    this.serviceName = serviceName;
  }

  // Need to check if the sender is trustworthy
  // Need to verify if some person that receives this message is from this domain

  private String getDomain(String address) {
    return address.substring(address.lastIndexOf("@") + 1);
  }

  private String getOnlyTheName(String address) {
    return address.split("@")[0];
  }

  private synchronized RestUsersClient getLocalUsersClient() {
    if (localUsersClient != null) {
      return localUsersClient;
    }

    // First time searching for users client
    String serviceName = "Users";
    URI[] uris = d.knownUrisOf(serviceName, this.domain, 1);

    if (uris == null || uris.length == 0) {

      throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);
    }
    localUsersClient = new RestUsersClient(uris[0]);

    return localUsersClient;
  }

  private verifyIfUserIsInServer(String pwd, String sender_name, String ){
    RestUsersClient usersClient = getLocalUsersClient();
    Result<List<User>> result = usersClient.searchUsers(, PWD, PATH)  
  }

  @Override
  public String postMessage(String pwd, Message msg) {

    Log.info("postMessage : pwd = " + pwd + "; msg = " + msg);

    if (pwd == null || msg == null) {
      Log.info("Password or Message null");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }
    String sender_name = msg.getSender();

    if (sender_name == null) {
      Log.info("Sender Name is none");
      throw new WebApplicationException(Status.BAD_REQUEST);
      // Message needs to be sent by this server case
    } else if (sender_name.matches(REGEX_LOCAL) && msg.getId() == null && getDomain(sender_name).equals(this.domain)) {

      URI usersServerUri = d.knownUrisOf("Users", this.domain, 1)[0];
      // Talk to the server to find the user object of the sender from this domain
      RestUsersClient usersClient = getLocalUsersClient();
      Result<User> result = usersClient.getUser(getOnlyTheName(sender_name), pwd);

      // Check if the sender exists in the users server
      if (result.isOK()) {
        Log.info("Found " + result.value().getName());
      } else {
        Log.info("Password is wrong or sender does not exist.");
        throw new WebApplicationException(Status.FORBIDDEN);
      }
      // Assign the ID
      // Modify the sender format
      String newId = UUID.randomUUID().toString();
      msg.setId(newId);

      String new_sender_name = "<" + sender_name + ">";
      msg.setSender(new_sender_name);

    } // Message sent by other server case
    else if (sender_name.matches(REGEX_FORWARDING) && msg.getId() != null) {
      // nothing to do here
    } else {
      Log.info("Sender name pattern is wrong");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }
    // LAST STEP: see the list of the destinations and deliver the ones that are in
    // this domain
    // Message already sent by someone
    // THIS IS WRONG
    Set<String> destinations = msg.getDestination();
    for (String destination : destinations) {
      // if the destination is local
      if (destination.matches(REGEX_LOCAL)) {
        if (getDomain(destination).equals(this.domain)) {
          mailBox.computeIfAbsent(getOnlyTheName(destination), k -> new ConcurrentHashMap<>())
              .put(msg.getId(), msg);
        }
      }
    }

  }
}