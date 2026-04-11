package sd2526.trab.server.java;

import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Users;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.discovery.Discovery;
import sd2526.trab.server.persistence.Hibernate;
import sd2526.trab.api.UserInbox;
import sd2526.trab.client.ClientFactory;
import sd2526.trab.client.rest.RestMessagesClient;
import sd2526.trab.client.rest.RestUsersClient;
import sd2526.trab.client.grpc.GrpcMessagesClient;
import sd2526.trab.client.grpc.GrpcUsersClient;

import org.hibernate.exception.ConstraintViolationException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class JavaMessages implements Messages {
  private static final Logger Log = Logger.getLogger(JavaMessages.class.getName());
  private static final boolean POSTMESSAGE = true;
  private static final boolean DELETEMESSAGE = false;
  private static final String REGEX_LOCAL = "^[^@<>\\s]+@[^@<>\\s]+$";
  private static final String REGEX_FORWARDING = "^.+\\s<[^@<>\\s]+@[^@<>\\s]+>$";

  private final Discovery d;
  private final String domain;
  private final Hibernate hibernate;

  // System constants and Remote messages
  private static final String SYSTEM_SENDER = "<System>";
  private static final String DELETED_SENDER = "<Deleted>";
  private static final String PROBE_PWD_PREFIX = "Sonda";

  // Timeouts
  private static final long FAST_THREAD_TIMEOUT_MS = 2000L;
  private static final long BACKGROUND_THREAD_TIMEOUT_MS = 90000L;
  private static final long BACKGROUND_MIN_REMAINING_MS = 35000L;
  private static final long MAX_DELETE_AGE_MS = 30000L;


  // Retries and waits
  private static final int MAX_DB_RETRIES = 3;
  private static final int MAX_PROBE_RETRIES = 5;
  private static final long SLEEP_PROBE_RETRY_MS = 200L;
  private static final long SLEEP_DB_RETRY_BASE_MS = 50L;
  private static final long INBOX_READ_RETRY_MS = 500L; 

  private final Map<String, Messages> externalClients = new ConcurrentHashMap<>();

  private final ClientFactory<Users> usersFactory;
  private final ClientFactory<Messages> messagesFactory;

  public JavaMessages(String serviceName, String domain, Discovery discovery) {
    this.d = discovery;
    this.domain = domain;
    this.hibernate = Hibernate.getInstance();

    this.usersFactory = new ClientFactory<>(Users.SERVICE_NAME, RestUsersClient::new, GrpcUsersClient::new);
    this.messagesFactory = new ClientFactory<>(Messages.SERVICE_NAME, RestMessagesClient::new, GrpcMessagesClient::new);
  }

  private String getDomain(String address) {
    address = formatSenderNameFromMsg(address);
    return address.substring(address.lastIndexOf("@") + 1);
  }

  private String getOnlyTheName(String address) {
    if (address.contains(" ")) {
      return address.split(" ")[address.split(" ").length - 1].split("@")[0];
    }
    return address.split("@")[0];
  }

  private String formatSenderNameFromMsg(String sender) {
    if (sender.contains(" "))
      sender = sender.split(" ")[sender.split(" ").length - 1];
    if (sender.contains("<"))
      return sender.substring(1, sender.length() - 1);
    return sender;
  }

  private boolean checkIfUserIsFromDomain(String destination) {
    return destination.matches(REGEX_LOCAL) && getDomain(destination).equals(this.domain);
  }

  private Messages getExternalClient(String targetDomain) {
    if (externalClients.containsKey(targetDomain))
      return externalClients.get(targetDomain);
    try {
      Messages client = messagesFactory.get(d, targetDomain);
      externalClients.put(targetDomain, client);
      return client;
    } catch (Exception e) {
      throw new RuntimeException("Gateway/Messages server unavailable for domain " + targetDomain);
    }
  }

  private boolean checkUserExistenceAndPassword(String name, String pwd) {
    if (name == null)
      return false;
      
    // The internal system does not need password validation
    if (name.equals(SYSTEM_SENDER))
      return true;

    if (name.matches(REGEX_LOCAL) && getDomain(name).equals(this.domain)) {
      // If the user is from our domain, we call the Users service to validate credentials
      try {
        Users usersClient = usersFactory.get(d, this.domain);
        Result<User> result = usersClient.getUser(getOnlyTheName(name), pwd);
        return result.isOK() && result.value() != null;
      } catch (Exception e) {
        return false;
      }
    } else if (name.matches(REGEX_FORWARDING) && !getDomain(name).equals(this.domain)) {
      // If it's a forwarding user from another domain, 
      // we accept it upfront since actual validation happens at the destination server.
      return true;
    }
    
    return false; // Fail-safe: return false if no conditions are met
  }

  private Message generateErrorMessage(String failedDest, Message msg) {
    Message fail = new Message();
    
    // The error message ID is the original ID concatenated with the failed destination,
    // helping to quickly identify it as a Bounce/Failure Notification.
    fail.setId(msg.getId() + "." + failedDest);
    
    fail.setSender(msg.getSender());
    // The destination of the error message becomes the original sender (return to sender)
    fail.setDestination(Collections.singleton(formatSenderNameFromMsg(msg.getSender())));
    
    // Clear subject indicating the reason for the failure
    fail.setSubject("FAILED TO SEND " + msg.getId() + " TO " + failedDest + ": UNKNOWN USER");
    fail.setContents(msg.getContents());
    fail.setCreationTime(System.currentTimeMillis());
    
    return fail;
  }

  private void propagateMessage(String pwd, Message msg, List<String> destinations, boolean choice) {
    String sender = formatSenderNameFromMsg(msg.getSender());
    String cleanSender = getOnlyTheName(sender) + "@" + getDomain(sender);

    // 1. Group destinations by Domain to send messages in batches / minimize network calls
    Map<String, List<String>> domainsMap = new ConcurrentHashMap<>();
    for (String dest : destinations) {
      String targetDomain = getDomain(dest);
      domainsMap.computeIfAbsent(targetDomain, k -> new ArrayList<>()).add(dest);
    }

    // 2. Iterate over each target domain
    for (Map.Entry<String, List<String>> entry : domainsMap.entrySet()) {
      String targetDomain = entry.getKey();
      List<String> targetDestinations = entry.getValue();

      // Local handling (optimization: no need to hit the network)
      if (targetDomain.equals(this.domain)) {
        for (String dest : targetDestinations) {
          if (choice == POSTMESSAGE) {
            assignMessageToUsers(dest, pwd, msg);
          } else {
            deleteMessageFromUsers(dest, pwd, msg);
          }
        }
        continue;
      }

      final long startTime = System.currentTimeMillis();

      // 3. First attempt: Fast Synchronous Thread (with a tight timeout)
      try {
        Messages msgClient = getExternalClient(targetDomain);
        final Result<?>[] syncResult = new Result<?>[1];

        Thread fastThread = new Thread(() -> {
          try {
            if (choice == POSTMESSAGE) {
              syncResult[0] = msgClient.postMessage(pwd, msg);
            } else {
              syncResult[0] = msgClient.deleteMessage(cleanSender, msg.getId(), pwd);
            }
          } catch (Exception e) {
            // Ignore network errors here, as the timeout or background thread will handle them
          }
        });

        fastThread.start();
        fastThread.join(FAST_THREAD_TIMEOUT_MS);

        if (fastThread.isAlive()) {
          fastThread.interrupt();
          throw new RuntimeException("Timeout of 2s hitted!");
        }

        Result<?> result = syncResult[0];
        if (result != null) {
          if (result.isOK()) {
            continue; // Success in the synchronous thread, skip to the next domain
          } else if (result.error() == ErrorCode.NOT_FOUND ||
              result.error() == ErrorCode.FORBIDDEN ||
              result.error() == ErrorCode.BAD_REQUEST) {

            // Definitive errors (4xx). If it's a POST and not already a Bounce, generate a failure notification.
            if (choice == POSTMESSAGE && !msg.getId().contains(".")) {
              for (String failedDest : targetDestinations) {
                Message notification = generateErrorMessage(failedDest, msg);
                propagateMessage(pwd, notification, Collections.singletonList(cleanSender), POSTMESSAGE);
              }
            }
            continue; // Error handled, skip to the next domain
          }
        } else {
          throw new RuntimeException("Result null");
        }
      } catch (Exception e) {
        Log.info("Synchronous attempt aborted for " + targetDomain + ". Moving to Background Thread...");
      }

      // 4. Fallback: Background Thread for long-running retries (tolerance to temporary failures)
      new Thread(() -> {
        boolean success = false;
        boolean unknownUser = false;

        while (true) {
          long elapsed = System.currentTimeMillis() - startTime;
          if (elapsed >= BACKGROUND_THREAD_TIMEOUT_MS)
            break;

          // Protection against excessively short wait times or infinite loops stuck at the end of the timeout
          if (BACKGROUND_THREAD_TIMEOUT_MS - elapsed < BACKGROUND_MIN_REMAINING_MS) {
            try {
              long sleepTime = BACKGROUND_THREAD_TIMEOUT_MS - elapsed;
              if (sleepTime > 0)
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) { }
            break;
          }

          try {
            Messages msgClient = getExternalClient(targetDomain);
            Result<?> result;

            if (choice == POSTMESSAGE) {
              result = msgClient.postMessage(pwd, msg);
            } else {
              result = msgClient.deleteMessage(cleanSender, msg.getId(), pwd);
            }

            if (result.isOK()) {
              success = true;
              break;
            } else if (result.error() == ErrorCode.NOT_FOUND ||
                result.error() == ErrorCode.FORBIDDEN ||
                result.error() == ErrorCode.BAD_REQUEST) {
              if (result.error() == ErrorCode.NOT_FOUND)
                unknownUser = true;
              break; // Definitive failure, no point in continuing to retry
            }
          } catch (Exception e) { }

          try {
            Thread.sleep(100); // Light backoff before the next retry in the loop
          } catch (InterruptedException e) {
            break;
          }
        }

        // 5. If it failed definitively after all retries and isn't already a Bounce notice
        if (!success && choice == POSTMESSAGE && !msg.getId().contains(".")) {
          Log.info("TIMEOUT REACHED (90s): Generating failed notification to " + sender);

          for (String failedDest : targetDestinations) {
            Message notification = generateErrorMessage(failedDest, msg);
            if (!unknownUser) {
              notification.setSubject("FAILED TO SEND " + msg.getId() + " TO " + failedDest + ": TIMEOUT");
            }

            // Try to deliver the notification locally first, fallback to normal propagation
            try {
              Messages localClient = getExternalClient(getDomain(cleanSender));
              localClient.postMessage(pwd, notification);
            } catch (Exception e) {
              propagateMessage(pwd, notification, Collections.singletonList(cleanSender), POSTMESSAGE);
            }
          }
        }
      }).start();
    }
  }

  private Result<Void> assignMessageToUsers(String des, String pwd, Message msg) {
    // Retry loop to mitigate DB concurrency issues (ConstraintViolations or Locks)
    for (int retry = 0; retry < MAX_DB_RETRIES; retry++) {
      // Synchronize using the destination string from the pool (intern) to prevent 
      // two threads from trying to create the same user's inbox simultaneously
      synchronized (des.intern()) {
        try {
          UserInbox usrIn = hibernate.get(UserInbox.class, des);

          if (usrIn == null) {
            String senderRaw = formatSenderNameFromMsg(msg.getSender());
            boolean isSystem = SYSTEM_SENDER.equals(senderRaw);
            boolean isBounceNotice = msg.getId() != null && msg.getId().contains(".");
            String targetName = getOnlyTheName(des);
            boolean targetExists = false;

            // If it's from the System or an Error Notification, we assume the destination 
            // is valid to ensure critical alerts are delivered
            if (isSystem || isBounceNotice) {
              targetExists = true;
            } else {
              // Otherwise, we use a "Probe User" to test if the target exists in the Users microservice.
              // We try to create a fake user. If it returns CONFLICT, the target already exists.
              Users usersClient = usersFactory.get(d, this.domain);
              String tempPwd = UUID.randomUUID().toString();
              User probeUser = new User(targetName, tempPwd, PROBE_PWD_PREFIX, this.domain);

              targetExists = false;

              for (int i = 0; i < MAX_PROBE_RETRIES; i++) {
                try {
                  Result<String> probeRes = usersClient.postUser(probeUser);

                  if (probeRes.isOK()) {
                    // Probe successfully created: the target DID NOT exist. We clean up the probe.
                    targetExists = false;
                    try {
                      usersClient.deleteUser(targetName, tempPwd);
                    } catch (Exception ignored) { }
                    break;
                  } else if (probeRes.error() == ErrorCode.CONFLICT) {
                    // Conflict: The target user EXISTS (which is what we wanted to confirm).
                    targetExists = true;
                    break;
                  } else if (probeRes.error() == ErrorCode.TIMEOUT || probeRes.error() == ErrorCode.INTERNAL_ERROR) {
                    // Transient errors: sleep and retry
                    if (i < MAX_PROBE_RETRIES - 1) {
                      Thread.sleep(SLEEP_PROBE_RETRY_MS);
                      continue;
                    } else {
                      // If we run out of retries due to timeouts, assume true to avoid losing the message
                      targetExists = true; 
                    }
                  } else {
                    targetExists = false;
                    break;
                  }
                } catch (Exception e) {
                  if (i < MAX_PROBE_RETRIES - 1) {
                    Thread.sleep(SLEEP_PROBE_RETRY_MS);
                  } else {
                    targetExists = true;
                  }
                }
              }
            }

            // If the target definitively does not exist, return NOT_FOUND
            if (!targetExists) {
              return Result.error(ErrorCode.NOT_FOUND);
            }

            // Create and persist the new Inbox for the user
            usrIn = new UserInbox(des);
            hibernate.persist(usrIn);
          }

          // Idempotency check: only add the message if it's not already in the Inbox
          Message obtained_msg_inbox = usrIn.checkIfMessageInInbox(msg.getId());
          if (obtained_msg_inbox == null) {
            usrIn.addMessage(msg);
            hibernate.update(usrIn);
          }

          return Result.ok();

        } catch (ConstraintViolationException e) {
          return Result.error(ErrorCode.CONFLICT);
        } catch (Exception e) {
          // Log and return error only on the last attempt
          if (retry == MAX_DB_RETRIES - 1) {
            Log.severe("Internal Database Error: " + e.getMessage());
            return Result.error(ErrorCode.INTERNAL_ERROR);
          }
          try {
            // Random backoff to desynchronize threads (jitter)
            Thread.sleep(SLEEP_DB_RETRY_BASE_MS + (int) (Math.random() * 50));
          } catch (InterruptedException ignored) {}
        }
      }
    }
    return Result.error(ErrorCode.INTERNAL_ERROR);
  }

  private Result<Void> deleteMessageFromUsers(String des, String pwd, Message msg) {
    String targetDomain = getDomain(des);

    if (targetDomain.equals(this.domain)) {
      try {
        UserInbox usrIn = hibernate.get(UserInbox.class, des);

        if (usrIn != null) {
          Message cur_message = usrIn.checkIfMessageInInbox(msg.getId());
          if (cur_message != null) {
            usrIn.removeMessage(msg.getId());
            hibernate.update(usrIn);
          }
        }
        return Result.ok();
      } catch (Exception e) {
        Log.severe("Failed to delete message locally: " + e.getMessage());
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }

    try {
      Messages msgClient = getExternalClient(targetDomain);
      return msgClient.deleteMessage(des, msg.getId(), pwd);
    } catch (Exception e) {
      Log.warning("Failed remote delete: " + e.getMessage());
      return Result.error(ErrorCode.TIMEOUT);
    }
  }

  @Override
  public Result<String> postMessage(String pwd, Message msg) {
    if (pwd == null || msg == null || msg.getSender() == null)
      return Result.error(ErrorCode.BAD_REQUEST);

    String sender_name = msg.getSender();
    boolean isNewMessage = (msg.getId() == null);
    boolean isLocalSender = getDomain(sender_name).equals(this.domain);
    boolean isBounceNotice = msg.getId() != null && msg.getId().contains(".");

    if (isLocalSender && !isBounceNotice && !checkUserExistenceAndPassword(sender_name, pwd)) {
      return Result.error(ErrorCode.FORBIDDEN);
    }

    if (isNewMessage) {
      if (!isLocalSender)
        return Result.error(ErrorCode.BAD_REQUEST);
      try {
        Users usersClient = usersFactory.get(d, this.domain);
        Result<User> userRes = usersClient.getUser(getOnlyTheName(sender_name), pwd);
        if (!userRes.isOK())
          return Result.error(ErrorCode.FORBIDDEN);

        msg.setId(UUID.randomUUID().toString());
        msg.setSender(userRes.value().getDisplayName() + " <" + sender_name + ">");
        msg.setCreationTime(System.currentTimeMillis());
        sender_name = msg.getSender();
      } catch (Exception e) {
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }

    String rawSenderEmail = formatSenderNameFromMsg(sender_name);
    boolean isOriginatingServer = getDomain(rawSenderEmail).equals(this.domain);

    List<String> remoteDestinations = new ArrayList<>();
    List<Message> failureNoticesToPropagate = new ArrayList<>();

    boolean persistSuccess = false;
    for (int retry = 0; retry < MAX_DB_RETRIES; retry++) {
      try {
        synchronized (msg.getId().intern()) {
          Message existing = hibernate.get(Message.class, msg.getId());
          if (existing == null) {
            hibernate.persist(msg);
          } else {
            if ("<Deleted>".equals(existing.getSender())) {
              return Result.ok(msg.getId());
            }
            hibernate.update(msg);
          }
        }
        persistSuccess = true;
        break;
      } catch (Exception e) {
        if (retry == 2) {
          Log.severe("Error persisting message: " + e.getMessage());
          return Result.error(ErrorCode.INTERNAL_ERROR);
        }
        try {
          Thread.sleep(SLEEP_DB_RETRY_BASE_MS + (int) (Math.random() * SLEEP_DB_RETRY_BASE_MS));
        } catch (InterruptedException ignored) {
        }
      }
    }

    if (!persistSuccess)
      return Result.error(ErrorCode.INTERNAL_ERROR);

    for (String destination : msg.getDestination()) {
      if (checkIfUserIsFromDomain(destination)) {
        Result<Void> res = assignMessageToUsers(destination, pwd, msg);
        if (!res.isOK() && res.error() == ErrorCode.NOT_FOUND) {
          Message failureNotice = generateErrorMessage(destination, msg);

          for (int retry = 0; retry < MAX_DB_RETRIES; retry++) {
            try {
              if (hibernate.get(Message.class, failureNotice.getId()) == null) {
                hibernate.persist(failureNotice);
              }
              break;
            } catch (Exception e) {
              try {
                Thread.sleep(SLEEP_DB_RETRY_BASE_MS);
              } catch (InterruptedException ignored) {
              }
            }
          }

          if (!msg.getId().contains(".")) {
            failureNoticesToPropagate.add(failureNotice);
          }
        }
      } else {
        if (isOriginatingServer || msg.getId().contains(".")) {
          remoteDestinations.add(destination);
        }
      }
    }

    if (!remoteDestinations.isEmpty()) {
      propagateMessage(pwd, msg, remoteDestinations, POSTMESSAGE);
    }

    for (Message failureNotice : failureNoticesToPropagate) {
      propagateMessage(pwd, failureNotice, Collections.singletonList(rawSenderEmail), POSTMESSAGE);
    }

    return Result.ok(msg.getId());
  }

  @Override
  public Result<Message> getInboxMessage(String name, String mid, String pwd) {
    if (name == null || mid == null || pwd == null)
      return Result.error(ErrorCode.BAD_REQUEST);

    String fullName = name.contains("@") ? name : name + "@" + this.domain;

    if (!checkUserExistenceAndPassword(fullName, pwd))
      return Result.error(ErrorCode.FORBIDDEN);

    try {

      UserInbox usrInbox = hibernate.get(UserInbox.class, fullName);
      Message msg = null;

      if (usrInbox != null) {
        msg = usrInbox.checkIfMessageInInbox(mid);
      }

      if (msg == null) {
        try {
          Thread.sleep(INBOX_READ_RETRY_MS);
        } catch (InterruptedException e) {

        }

        usrInbox = hibernate.get(UserInbox.class, fullName);
        if (usrInbox != null) {
          msg = usrInbox.checkIfMessageInInbox(mid);
        }
      }

      if (msg == null) {
        return Result.error(ErrorCode.NOT_FOUND);
      }

      return Result.ok(msg);
    } catch (Exception e) {
      return Result.error(ErrorCode.INTERNAL_ERROR);
    }
  }

  @Override
  public Result<List<String>> getAllInboxMessages(String name, String pwd) {
    return searchInbox(name, pwd, "");
  }

  @Override
  public Result<List<String>> searchInbox(String name, String pwd, String query) {
    if (name == null || pwd == null)
      return Result.error(ErrorCode.BAD_REQUEST);
    String fullName = name.contains("@") ? name : name + "@" + this.domain;

    if (!checkUserExistenceAndPassword(fullName, pwd))
      return Result.error(ErrorCode.FORBIDDEN);

    try {
      UserInbox inbox = hibernate.get(UserInbox.class, fullName);
      if (inbox == null)
        return Result.ok(new ArrayList<>());

      List<String> results = (query == null || query.trim().isEmpty())
          ? inbox.getMessages().stream().map(Message::getId).toList()
          : inbox.getMessages(query);

      return Result.ok(results);
    } catch (Exception e) {
      return Result.error(ErrorCode.INTERNAL_ERROR);
    }
  }

  @Override
  public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
    if (name == null || mid == null || pwd == null)
      return Result.error(ErrorCode.BAD_REQUEST);

    String fullName = name.contains("@") ? name : name + "@" + this.domain;

    if (!checkUserExistenceAndPassword(fullName, pwd))
      return Result.error(ErrorCode.FORBIDDEN);

    for (int i = 0; i < MAX_DB_RETRIES; i++) {
      try {
        synchronized (fullName.intern()) {
          UserInbox inbox = hibernate.get(UserInbox.class, fullName);
          if (inbox != null) {
            if (inbox.checkIfMessageInInbox(mid) != null) {
              inbox.removeMessage(mid);
              hibernate.update(inbox);
            }
          }
        }
        return Result.ok();
      } catch (Exception e) {
        if (i == 2) {
          Log.severe("Erro de concorrência no remove após algumas tentativas: " + e.getMessage());
          return Result.error(ErrorCode.INTERNAL_ERROR);
        }

        try {
          Thread.sleep(SLEEP_DB_RETRY_BASE_MS);
        } catch (InterruptedException ignored) {
        }
      }
    }
    return Result.ok();
  }

  @Override
  public Result<Void> deleteMessage(String name, String mid, String pwd) {
    if (name == null || mid == null || pwd == null) {
      return Result.error(ErrorCode.BAD_REQUEST);
    }

    String fullName = name.contains("@") ? name : name + "@" + this.domain;
    List<String> remoteDestinations = new ArrayList<>();
    Message msgToPropagate = null;

    for (int i = 0; i < MAX_DB_RETRIES; i++) {
      try {
        synchronized (mid.intern()) {
          Message msg = hibernate.get(Message.class, mid);

          if (msg == null) {
            Message tombstone = new Message();
            tombstone.setId(mid);
            tombstone.setSender("<Deleted>");
            tombstone.setCreationTime(0L);
            try {
              hibernate.persist(tombstone);
            } catch (Exception ignored) {
            }
            return Result.ok();
          }

          if ("<Deleted>".equals(msg.getSender())) {
            return Result.ok();
          }

          String rawSender = formatSenderNameFromMsg(msg.getSender());
          boolean isOriginatingServer = getDomain(rawSender).equals(this.domain);

          if (isOriginatingServer) {
            if (!checkUserExistenceAndPassword(fullName, pwd))
              return Result.error(ErrorCode.FORBIDDEN);
            if (!rawSender.equals(fullName))
              return Result.error(ErrorCode.FORBIDDEN);
          } else {
            if (!rawSender.equals(fullName))
              return Result.error(ErrorCode.FORBIDDEN);
          }

          if (isOriginatingServer && msg.getCreationTime() > 0) {
            long messageAge = System.currentTimeMillis() - msg.getCreationTime();
            if (messageAge > MAX_DELETE_AGE_MS) {
              Log.info("DeleteMessage: User request ignored, message is too old (" + (messageAge / 1000) + "s).");
              return Result.ok();
            }
          }

          for (String destination : msg.getDestination()) {
            if (checkIfUserIsFromDomain(destination)) {
              synchronized (destination.intern()) {
                UserInbox inbox = hibernate.get(UserInbox.class, destination);
                if (inbox != null) {
                  inbox.removeMessage(mid);
                  hibernate.update(inbox);
                }
              }
            } else if (isOriginatingServer) {
              remoteDestinations.add(destination);
            }
          }

          hibernate.delete(msg);
          msgToPropagate = msg;
        }
        break;

      } catch (Exception e) {
        if (i == 2) {
          Log.severe("Failed to delete message after algumas retries: " + e.getMessage());
          return Result.error(ErrorCode.INTERNAL_ERROR);
        }
        remoteDestinations.clear();
        msgToPropagate = null;
        try {
          Thread.sleep(SLEEP_DB_RETRY_BASE_MS + (int) (Math.random() * SLEEP_DB_RETRY_BASE_MS));
        } catch (InterruptedException ignored) {
        }
      }
    }

    if (!remoteDestinations.isEmpty() && msgToPropagate != null) {
      propagateMessage(pwd, msgToPropagate, remoteDestinations, DELETEMESSAGE);
    }

    return Result.ok();
  }
}