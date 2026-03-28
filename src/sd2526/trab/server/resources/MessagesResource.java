package sd2526.trab.server.resources;

import sd2526.*;
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

  private String getOnlyTheName(String address) {
    return address.split("@")[0];
  }

  private boolean checkIfUserIsFromDomain(String destination) {
    return destination.matches(REGEX_LOCAL) && getDomain(destination).equals(this.domain);
  }

  private String formatSenderNameFromMsg(Message msg) {
    String sender = msg.getSender();
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
    return usrInbox.checkIfMessageInInbox(msgId);
  }

  private synchronized RestMessagesClient getMessagesClient(Message msg, String domain) {
    if (messagesClients.containsKey(domain)) {
      return messagesClients.get(domain);
    }

    try {
      Result<URI[]> uris = d.knownUrisOf(this.serviceName, domain, 1);
      if (!uris.isOK()) {
        // This might be wrong, fix later
        // Timeout if server not being able to answer 5 seconds later
        Log.info("Server is not being able to delivering uris by multicast.");
        throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);
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
      // Deixamos passar o 503 para cima
      throw w;
    } catch (Exception e) {
      // Cortamos o mal pela raiz! Se der erro grave, atiramos 500 em vez de devolver
      // null
      e.printStackTrace();
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }
  }

  private void forwardMessage(Message msg, String destination_address, String pwd) {
    // if the destination is local and has the same domain
    if (destination_address.matches(REGEX_LOCAL)) {
      if (getDomain(destination_address).equals(this.domain)) {
        assignMessageToUsers(destination_address, pwd, msg, localUsersClient);
      } else {
        forwardMessageToOtherDomain(msg, destination_address);
      }
    } // The destination doesn't follow the pattern
    else {
      Log.info("The destination doesn't follow the pattern");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }
  }

  private void forwardMessageToOtherDomain(Message msg, String destination_address) {
    String targetDomain = getDomain(destination_address); // Ex: "tecnico"
    String sender = msg.getSender(); // Ex: "<nmp@fct>"
    String originalId = msg.getId();

    // New Thread to return the ID right away
    new Thread(() -> {
      long startTime = System.currentTimeMillis();
      long timeoutMillis = 90 * 1000;

      boolean success = false;
      boolean unknownUser = false;

      while ((System.currentTimeMillis() - startTime) < timeoutMillis) {
        try {
          // Search the Message server of the other domain
          Result<URI[]> uris = d.knownUrisOf("Messages", targetDomain, 1);

          if (uris.isOK() && uris.value().length > 0) {
            URI remoteUri = uris.value()[0];
            RestMessagesClient remoteClient = new RestMessagesClient(remoteUri);

            // Sends the message
            remoteClient.postMessage("", msg);

            Log.info("Success! Message" + originalId + " delivered at " + destination_address);
            success = true;
            break;
          } else {
            Log.info("Domain server " + targetDomain + " is down. Retrying...");
          }

        } catch (WebApplicationException e) {
          // The server answered, but rejected the message
          int status = e.getResponse().getStatus();
          if (status == Status.FORBIDDEN.getStatusCode() || status == Status.NOT_FOUND.getStatusCode()) {
            // The user doesn't exist in the domain
            Log.warning("The destiny user doesn't exist " + destination_address);
            unknownUser = true;
            break;
          }
        } catch (Exception e) {
          Log.warning("Net failed trying to contact " + targetDomain + ". Retrying...");
        }

        try {
          Thread.sleep(2000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }

      // Failed treatement
      if (!success) {
        Log.info("Generating a failed notification to the remetent " + sender);

        // Create a error message
        Message notification = new Message();
        notification.setId(originalId + "." + destination_address); // mid.user
        notification.setSender("System");
        notification.setDestination(new HashSet<>(Arrays.asList(sender))); // Returns the original remetent
        notification.setCreationTime(System.currentTimeMillis());
        notification.setContents(msg.getContents());

        if (unknownUser) {
          notification.setSubject("FAILED TO SEND " + originalId + " TO " + destination_address + ": UNKNOWN USER");
        } else {
          notification.setSubject("FAILED TO SEND " + originalId + " TO " + destination_address + ": TIMEOUT");
        }

        // Save the error notification in the data base
        try {
          String cleanSender = getOnlyTheName(sender) + "@" + getDomain(sender);
          assignMessageToUsers(cleanSender, "", notification, localUsersClient);
        } catch (Exception e) {
          Log.severe("Erro ao guardar a notificação de falha na caixa do remetente.");
        }
      }

    }).start();
  }

  private void assignMessageToUsers(String des, String pwd, Message msg, RestUsersClient usersClient) {
    // See if it already exists in the local server
    UserInbox usrIn = hibernate.get(UserInbox.class, des);
    if (usrIn == null) {

      String sender = formatSenderNameFromMsg(msg);
      // If does not exist in the local server check the user server to see if it
      // exists
      Result<List<User>> user = usersClient.searchUsers(sender, pwd, des);
      if (user.isOK()) {
        // Create a inbox for him
        // If it exists then create a inbox for that user
        try {
          hibernate.persist(user.value().get(0));
        } catch (ConstraintViolationException e) {
          e.printStackTrace();
          Log.info("User entry already exists in the inbox database");
        } catch (Exception x) {
          x.printStackTrace();
          throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
        }
        // The destination does not exist
      } else {
        // 1. Modify the id to and the subject
        Log.info("Message of the user failed.");
        msg.setSubject("FAILED TO SEND " + msg.getId() + " TO " + sender);
        msg.setId(msg.getId() + "." + sender);
        // 2. Send to the message to the mailbox of the Sender
        if (checkIfUserIsFromDomain(sender)) {
          UserInbox senderInbox = hibernate.get(UserInbox.class, sender);
        } else {
          forwardMessageToOtherDomain(msg, sender);
        }
      }

    }
    // Put the message in the mailbox
    usrIn.addMessage(msg);
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
    } else if (checkUserExistenceAndPassword(sender_name, pwd) && msg.getId() == null
        && getDomain(sender_name).equals(this.domain)) {

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
    // LAST STEP: Send the message to the destinations
    // Message already sent by someone
    Set<String> destinations = msg.getDestination();
    for (String destination : destinations) {
      // if the destination is local and has the same domain
      forwardMessage(msg, destination, pwd,);
    }
    // -----------------------------------------------------
    return msg.getId();
  }

  @Override
  public Message getMessage(String name, String mid, String pwd) {
    if (name == null || mid == null || pwd == null) {
      Log.info("Password or Message null");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }
    if (!checkUserExistenceAndPassword(name, pwd)) {
      Log.info("User doesn't exist or password is wrong");
      throw new WebApplicationException(Status.FORBIDDEN);
    }
    // This method can be null
    Message msg = checkIfMessageExist(mid, name);
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
      Log.info("Name ou pwd são nulos.");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    // Confirm if the user exists and the pwd is right
    if (!checkUserExistenceAndPassword(name, pwd)) {
      Log.info("User doesn't exist or password is wrong");
      throw new WebApplicationException(Status.FORBIDDEN);
    }

    // Get the InBox from the data base
    UserInbox inbox = null;
    try {
      inbox = hibernate.get(UserInbox.class, name);
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

    // Check if the password is right and the user exists
    if (!checkUserExistenceAndPassword(name, pwd)) {
      Log.info("User doesn't exist or password is wrong");
      throw new WebApplicationException(Status.FORBIDDEN);
    }

    try {
      // Get the inBox of the user
      UserInbox inbox = hibernate.get(UserInbox.class, name);

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
      Log.info("Name ou pwd são nulos.");
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

    // Check if the password is right and the user exists
    if (!checkUserExistenceAndPassword(name, pwd)) {
      Log.info("User doesn't exist or password is wrong");
      throw new WebApplicationException(Status.FORBIDDEN);
    }

    // Get the InBox from the data base
    UserInbox inbox = null;
    try {
      inbox = hibernate.get(UserInbox.class, name);
    } catch (Exception e) {
      Log.severe("Error on accessing the data base: " + e.getMessage());
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }

    return inbox.getMessages(query);
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