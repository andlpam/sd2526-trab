package sd2526.trab.server.resources;

import sd2526.trab.client.rest.RestMessagesClient;
import sd2526.trab.client.rest.RestUsersClient;
import sd2526.trab.discovery.Discovery;
import sd2526.trab.api.java.*;
import sd2526.trab.api.rest.RestMessages;
import java.util.UUID;
import java.util.logging.Logger;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.api.*;
import jakarta.validation.ConstraintViolationException;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import sd2526.trab.server.persistence.Hibernate;

public class MessagesResource implements RestMessages {
  private final Discovery d;
  private final String domain;
  private final String serviceName;
  private static Logger Log = Logger.getLogger(MessagesResource.class.getName());
  private static final long DELETE_MESSAGE_TIME = 30000;
  private static final boolean POSTMESSAGE = true;
  private static final boolean DELETEMESSAGE = false;

  // Can only contain one @
  private static final String REGEX_LOCAL = "^[^@<>\\s]+@[^@<>\\s]+$";

  private static final String REGEX_FORWARDING = "^.+\\s<[^@<>\\s]+@[^@<>\\s]+>$";

  private Map<String, RestMessagesClient> messagesClients;

  private RestUsersClient localUsersClient = null;

  private final Hibernate hibernate;

  public MessagesResource(String serviceName, String domain, Discovery discovery) {
    this.d = discovery;
    this.domain = domain;
    this.serviceName = serviceName;
    hibernate = Hibernate.getInstance();
    this.messagesClients = new ConcurrentHashMap<>();
  }

  // Need to check if the sender is trustworthy
  // Need to verify if some person that receives this message is from this domain

  private String getDomain(String address) {
    return address.substring(address.lastIndexOf("@") + 1);
  }

  // It can have the display name if it is from other domain or not.
  private String getOnlyTheName(String address) {
    String[] tmp = null;
    if (address.contains(" ")) {
      tmp = address.split(" ");
      return tmp[tmp.length - 1].split("@")[0];
    }
    return address.split("@")[0];
  }

  private boolean checkIfUserIsFromDomain(String destination) {
    return destination.matches(REGEX_LOCAL) && getDomain(destination).equals(this.domain);
  }

  // It can have the display name so we need to remove it
  private String formatSenderNameFromMsg(Message msg) {
    String[] tmp = null;
    String sender = msg.getSender();
    if (sender.contains(" ")) {
      tmp = sender.split(" ");
      sender = tmp[tmp.length - 1];

    }
    return sender.substring(1, sender.length() - 1);
  }

  private boolean checkUserExistenceAndPassword(String name, String pwd) {
    if (name == null) {
      Log.info("Sender Name is none");
      return false;
      // Message needs to be sent by this server case
    } else if (name.matches(REGEX_LOCAL) && getDomain(name).equals(this.domain)) {

      // Talk to the server to find the user object of the sender from this domain
      RestUsersClient usersClient = getLocalUsersClient();
      Result<User> result = usersClient.getUser(getOnlyTheName(name), pwd);

      // If the user is not created yet create the user

      // Check if the sender exists in the users server
      if (result.isOK()) {
        Log.info("Found " + result.value().getName());
        return true;
      }
    }
    Log.info("Password is wrong or sender does not exist.");
    return false;
    // Assign the ID
  }

  private boolean checkIfMessageExist(Message msg, String userName) {
    UserInbox usrInbox = hibernate.get(UserInbox.class, userName);
    return usrInbox.checkIfMessageInInbox(msg);
  }

  private Message checkIfMessageExist(String msgId, String userName) {
    UserInbox usrInbox = hibernate.get(UserInbox.class, userName);
    if (usrInbox == null) {
      return null;
    }
    return usrInbox.checkIfMessageInInbox(msgId);
  }

  // WE NEED A FUNCTION THAT HANDLES NON-EXISTING DESTINATION
  // WE NEED A FUNCTION THAT HANDLES SENDING
  private synchronized RestMessagesClient getMessagesClient(Message msg, String domain) {
    if (messagesClients.containsKey(domain)) {
      return messagesClients.get(domain);
    }

    try {
      Result<URI[]> uris = d.knownUrisOf("Gateway", domain, 1);
      if (!uris.isOK()) {
        // This might be wrong, fix later
        // Might not have a gateway server
        uris = d.knownUrisOf(this.serviceName, domain, 1);
        if (!uris.isOK()) {
          Log.info("Gateway and Messages server are unavailable.");
          throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);
        }

      }
      messagesClients.putIfAbsent(domain, new RestMessagesClient(uris.value()[0]));

    } catch (Exception e) {
      e.printStackTrace();
      throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);
    }
    return messagesClients.get(domain);

  }

  private synchronized RestUsersClient getLocalUsersClient() {
    if (localUsersClient != null) {
      return localUsersClient;
    }

    // First time searching for users client
    String serviceName = "Users";
    try {
      Result<URI[]> uris = d.knownUrisOf(serviceName, this.domain, 1);

      if (!uris.isOK()) {
        // This might be wrong, fix later
        // Timeout if server not being able to answer 5 seconds later
        Log.info("Server is not being able to delivering uris by multicast.");
        throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);
      }
      localUsersClient = new RestUsersClient(uris.value()[0]);
      return localUsersClient;

    } catch (WebApplicationException w) {

      throw w;
    } catch (Exception e) {

      e.printStackTrace();
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }
  }

  private void propagateMessage(String pwd, Message msg, String destination_address, boolean choice) {
    String targetDomain = getDomain(destination_address); // Ex: "tecnico"
    String sender = formatSenderNameFromMsg(msg); // Ex: "<nmp@fct>"
    String originalId = msg.getId();

    // New Thread to return the ID right away
    new Thread(() -> {
      long startTime = System.currentTimeMillis();
      long timeoutMillis = 90 * 1000;

      boolean success = false;
      boolean unknownUser = false;

      while ((System.currentTimeMillis() - startTime) < timeoutMillis) {
        try {
          // IMPORTANT....
          // Here we are going to have a function to decide if we are going to ask to the
          // user server or to other
          // Messages server
          Result<Void> result;
          if (choice) {
            result = assignMessageToUsers(destination_address, pwd, msg);
          } else {
            result = deleteMessageFromUsers(destination_address, pwd, msg);
          }
          // Everything well
          if (result.isOK()) {
            success = true;
            break;
            // It doesn't need to try again because the error have no relation with the
            // server condition
          } else if (result.error() == ErrorCode.NOT_FOUND || result.error() == ErrorCode.FORBIDDEN
              || result.error() == ErrorCode.BAD_REQUEST) {
            unknownUser = true;
            break;
          }
          // The server is having a temporary error let's try again later
          Thread.sleep(3000);
          // The server had a processing error or is unvailable let's try again later
        } catch (Exception e) {
          Log.warning("Failed attempt : " + e);
          try {
            Thread.sleep(3000);
          } catch (InterruptedException x) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
      // Failed treatement
      if (!success) {
        Log.info("Generating a failed notification to the remetent " + sender);

        // Create a error message
        Message notification = new Message();
        notification.setId(originalId + "." + destination_address); // mid.user
        notification.setSender("<System>");
        notification.setDestination(new HashSet<>(Arrays.asList(sender))); // Returns the original remetent
        notification.setCreationTime(System.currentTimeMillis());
        notification.setContents(msg.getContents());

        if (unknownUser) {
          notification.setSubject("FAILED TO SEND " + originalId + " TO " + destination_address + ": UNKNOWN USER");
        } else {
          notification.setSubject("FAILED TO SEND " + originalId + " TO " + destination_address + ": TIMEOUT");
        }

        // Save the error notification in the data base

        String cleanSender = getOnlyTheName(sender) + "@" + getDomain(sender);
        Result<Void> senderResult = assignMessageToUsers(cleanSender, pwd, notification);
        if (senderResult.isOK())
          Log.info("Error message delivered to the sender.");
        else {
          Log.severe("Error message did not arrive to the sender.");
        }
      }

    }).start();
  }

  private Result<Void> assignMessageToUsers(String des, String pwd, Message msg) {
    String targetDomain = getDomain(des);
    if (targetDomain.equals(this.domain)) {
      UserInbox usrIn = null;
      // See if it already exists in the local server
      try {
        usrIn = hibernate.get(UserInbox.class, des);
        if (usrIn == null) {
          String sender = formatSenderNameFromMsg(msg);
          // If does not exist in the local server check the user server to see if it
          // exists
          RestUsersClient usersClient = getLocalUsersClient();
          Result<List<User>> user = usersClient.searchUsers(getOnlyTheName(sender), pwd, des);
          if (user.isOK()) {

            // Create a inbox for him
            // If it exists then create a inbox for that user
            try {
              usrIn = new UserInbox(des);
              hibernate.persist(usrIn);
            } catch (ConstraintViolationException e) {
              e.printStackTrace();
              Log.info("User entry already exists in the inbox database");
              return Result.error(ErrorCode.CONFLICT);
            } catch (Exception x) {
              x.printStackTrace();
              return Result.error(ErrorCode.INTERNAL_ERROR);
            }
            // The destination does not exist
          } else {
            return Result.error(ErrorCode.NOT_FOUND);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }

      try {
        // In the first time he inserts the message and in the others he can update the
        // msg or do nothing about it.
        hibernate.update(msg);

        usrIn.addMessage(msg);

        hibernate.update(usrIn);

      } catch (Exception e) {
        Log.severe("Error saving in the database " + e.getMessage());
        e.printStackTrace();
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
      return Result.ok();
    }
    // It is from other domain the destination
    RestMessagesClient msgClient = getMessagesClient(msg, targetDomain);
    Result<String> result = msgClient.postMessage(pwd, msg);
    if (result.isOK()) {
      return Result.ok();
    }
    // This might be an error-----------------------------
    else
      return Result.error(result);
  }

  private Result<Void> deleteMessageFromUsers(String des, String pwd, Message msg) {

    String targetDomain = getDomain(des);
    if (targetDomain.equals(this.domain)) {
      UserInbox usrIn = null;
      // See if the mailbox user exists
      try {
        usrIn = hibernate.get(UserInbox.class, des);

        if (usrIn != null) {
          if (usrIn.checkIfMessageInInbox(msg))
            removeFromUserInbox(getOnlyTheName(des), msg.getId(), pwd);
          // If the mailbox exists but there is no message there with that id it's ok also

        } // If it doesnt have a mailbox it is ok because the message doesnt exist
        return Result.ok();

      } catch (Exception e) {
        e.printStackTrace();
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
    RestMessagesClient msgClient = getMessagesClient(msg, targetDomain);

    try {
      Result<Void> result = msgClient.deleteMessage(des, msg.getId(), pwd);
      return result;
    } catch (Exception e) {
      e.printStackTrace();
      // We dont know the cause of the failure so we cannot give an exact status..
      return Result.error(ErrorCode.TIMEOUT);
    }
  }

  @Override
  public String postMessage(String pwd, Message msg) {

    if (pwd == null || msg == null) {
      Log.info("Password or Message null");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    Log.info("postMessage : pwd = " + pwd + "; msg = " + msg);
    String sender_name = msg.getSender();

    if (sender_name == null) {
      Log.info("Sender Name is none");
      throw new WebApplicationException(Status.BAD_REQUEST);
      // Message needs to be sent by this server case
    } else if (!checkUserExistenceAndPassword(sender_name, pwd)) {
      Log.info("Password is incorrect or user does not exist.");
      throw new WebApplicationException(Status.FORBIDDEN);
    } else if (msg.getId() == null
        && getDomain(sender_name).equals(this.domain)) {

      // Assign the ID
      // Modify the sender format
      // Do a new creation time
      // He needs to see if the message is duplicated DO LATER....
      RestUsersClient usersClient = getLocalUsersClient();
      Result<User> result = usersClient.getUser(getOnlyTheName(sender_name), pwd);

      if (!result.isOK()) {
        Log.info("Password is incorrect or user does not exist.");
        throw new WebApplicationException(Status.FORBIDDEN);
      }

      String newId = UUID.randomUUID().toString();
      msg.setId(newId);

      String new_sender_name = result.value().getDisplayName() + " <" + sender_name + ">";

      msg.setSender(new_sender_name);

      msg.setCreationTime(System.currentTimeMillis());

    } // Message sent by other server case
    else if (sender_name.matches(REGEX_FORWARDING) && msg.getId() != null) {
      // nothing to do here
    } else {
      Log.info("Sender name pattern is wrong");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }
    // LAST STEP: Send the message to the destinations
    // Message already sent by someone
    Set<String> destinations = msg.getDestination();
    for (String destination : destinations) {
      // if the destination is local and has the same domain
      if (checkIfUserIsFromDomain(destination)) {

        Result<Void> res = assignMessageToUsers(destination, pwd, msg);
        if (!res.isOK()) {
          Log.info("Failed to persist local message");
          throw new WebApplicationException(GatewayResource.getStatusFromErrorCode(res.error()));
        }
      } else {
        propagateMessage(pwd, msg, destination, POSTMESSAGE);
      }

    }

    return msg.getId();
  }

  @Override
  public Message getMessage(String name, String mid, String pwd) {
    if (name == null || mid == null || pwd == null) {
      Log.info("Password or Message null");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }
    String fullName = name + "@" + this.domain;
    if (!checkUserExistenceAndPassword(fullName, pwd)) {
      Log.info("User doesn't exist or password is wrong");
      throw new WebApplicationException(Status.FORBIDDEN);
    }
    // This method can be null
    Message msg = checkIfMessageExist(mid, fullName);
    if (msg == null) {
      Log.info("The message doesn't exist.");
      throw new WebApplicationException(Status.NOT_FOUND);
    }
    return msg;
  }

  // 1.Verify if the user exists with the password provided
  // 2.Get all the messages of the inbox of the user
  private List<String> getAllInboxMessages(String name, String pwd) {
    Log.info("getAllInboxMessages : name = " + name + "; pwd = " + pwd);
    if (name == null || pwd == null) {
      Log.info("Name or pwd are null.");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }
    String fullName = name + "@" + this.domain;
    // Confirm if the user exists and the pwd is right
    if (!checkUserExistenceAndPassword(fullName, pwd)) {
      Log.info("User doesn't exist or password is wrong");
      throw new WebApplicationException(Status.FORBIDDEN);
    }

    // Get the InBox from the data base
    UserInbox inbox = null;
    try {
      inbox = hibernate.get(UserInbox.class, fullName);
    } catch (Exception e) {
      Log.info("Error on accessing the data base: " + e.getMessage());
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }

    // Extract messages ID
    List<String> messageIds = new ArrayList<>();

    // If the inbox exists and has messages inside we save the ID's
    if (inbox != null && inbox.getMessages() != null) {
      for (Message msg : inbox.getMessages()) {
        messageIds.add(msg.getId());
      }
    }

    // Return the list
    return messageIds;
  }

  // 1.First we need to check if the user is the user exists in the server
  // 2.Second we need to go to the user inbox and just remove it
  @Override
  public void removeFromUserInbox(String name, String mid, String pwd) {

    Log.info("removeFromUserInbox : name = " + name + "; mid = " + mid + "; pwd = " + pwd);
    if (name == null || mid == null || pwd == null) {
      Log.info("Params can be null.");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }
    String fullName = name + "@" + this.domain;
    // Check if the password is right and the user exists
    if (!checkUserExistenceAndPassword(fullName, pwd)) {
      Log.info("User doesn't exist or password is wrong");
      throw new WebApplicationException(Status.FORBIDDEN);
    }

    try {
      // Get the inBox of the user
      UserInbox inbox = hibernate.get(UserInbox.class, fullName);

      // If the inBox doesn't exist, the message isn't there
      if (inbox == null || inbox.getMessages().isEmpty()) {
        Log.info("The mail box is Empty");
        throw new WebApplicationException(Status.NOT_FOUND);
      }

      Message msg = inbox.checkIfMessageInInbox(mid);
      if (msg == null) {
        Log.info("Message " + mid + " wasn't found in the box " + name);
        throw new WebApplicationException(Status.NOT_FOUND);
      }
      inbox.removeMessage(msg);
      try {
        hibernate.update(inbox);
      } catch (Exception e) {
        Log.info("Database couldn't update");
        throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
      }

      Log.info("Message " + mid + " removed with success!");

    } catch (Exception e) {
      Log.info("Internal error in the data base with the message " + e.getMessage());
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }

  }

  @Override
  public List<String> getMessages(String name, String pwd, String query) {
    Log.info("getMessages : name = " + name + "; pwd = " + pwd + "; query = " + query);

    if (name == null || pwd == null) {
      Log.info("Name or pwd are null.");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    if (query == null || query.trim().isEmpty()) {
      Log.info("Empty query, returning messages");
      return getAllInboxMessages(name, pwd);
    } else {
      Log.info("Query found, starting search in " + query);
      return searchInbox(name, pwd, query);
    }
  }

  private List<String> searchInbox(String name, String pwd, String query) {
    if (name == null || pwd == null || query == null) {
      Log.info("Name or pwd or query are null");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }
    String fullName = name + "@" + this.domain;
    // Check if the password is right and the user exists
    if (!checkUserExistenceAndPassword(fullName, pwd)) {
      Log.info("User doesn't exist or password is wrong");
      throw new WebApplicationException(Status.FORBIDDEN);
    }

    // Get the InBox from the data base
    UserInbox inbox = null;
    try {
      inbox = hibernate.get(UserInbox.class, fullName);
    } catch (Exception e) {
      Log.severe("Error on accessing the data base: " + e.getMessage());
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }

    return inbox.getMessages(query);
  }

  @Override
  public void deleteMessage(String name, String mid, String pwd) {

    if (name == null || mid == null || pwd == null) {
      Log.info("Name or pwd or query are null");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }
    String fullName = name + "@" + this.domain;
    if (!checkUserExistenceAndPassword(fullName, pwd)) {
      Log.info("User doesn't exist or password is wrong");
      throw new WebApplicationException(Status.FORBIDDEN);
    }
    try {
      Message cur_message = hibernate.get(Message.class, mid);

      if (cur_message.getCreationTime() + DELETE_MESSAGE_TIME <= System.currentTimeMillis()) {
        Log.info("It is not possible to delete the message now. It does not generate an error.");
      } else {
        Set<String> destinations = cur_message.getDestination();
        for (String des : destinations) {
          propagateMessage(pwd, cur_message, des, DELETEMESSAGE);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }
  }

  public static ErrorCode getErrorCodeFrom(int status) {
    return switch (status) {
      case 200, 209 -> ErrorCode.OK;
      case 409 -> ErrorCode.CONFLICT;
      case 403 -> ErrorCode.FORBIDDEN;
      case 404 -> ErrorCode.NOT_FOUND;
      case 400 -> ErrorCode.BAD_REQUEST;
      case 500 -> ErrorCode.INTERNAL_ERROR;
      case 501 -> ErrorCode.NOT_IMPLEMENTED;
      default -> ErrorCode.INTERNAL_ERROR;
    };
  }

}