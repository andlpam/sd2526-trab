package sd2526.trab.server.rest.resources;

import sd2526.trab.client.grpc.GrpcMessagesClient;
import sd2526.trab.client.grpc.GrpcUsersClient;
import sd2526.trab.client.rest.RestGatewayClient;
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
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.UUID;
import sd2526.trab.server.persistence.Hibernate;

public class MessagesResource implements RestMessages {
  private final Discovery d;
  private final String domain;
  private final String serviceName;
  private static Logger Log = Logger.getLogger(MessagesResource.class.getName());
  private static final boolean POSTMESSAGE = true;
  private static final boolean DELETEMESSAGE = false;

  // Can only contain one @
  private static final String REGEX_LOCAL = "^[^@<>\\s]+@[^@<>\\s]+$";

  private static final String REGEX_FORWARDING = "^.+\\s<[^@<>\\s]+@[^@<>\\s]+>$";

  private Map<String, Messages> externalClients;

  private Users localUsersClient = null;

  private final Hibernate hibernate;

  public MessagesResource(String serviceName, String domain, Discovery discovery) {
    this.d = discovery;
    this.domain = domain;
    this.serviceName = serviceName;
    hibernate = Hibernate.getInstance();
    this.externalClients = new ConcurrentHashMap<>();
  }

  // Need to check if the sender is trustworthy
  // Need to verify if some person that receives this message is from this domain
  // @pre adress needs to be of type adds@domain and not display name +
  // <adds@domain>
  private String getDomain(String address) {
    address = formatSenderNameFromMsg(address);
    return address.substring(address.lastIndexOf("@") + 1);
  }

  private Message generateErrorMessage(String failedDestination, Message msg) {
    Message failureNotice = new Message();
    String failureId = msg.getId() + "." + failedDestination;
    failureNotice.setId(failureId);
    // O sender do erro é o mesmo da mensagem original
    failureNotice.setSender(msg.getSender());
    failureNotice.setDestination(Collections.singleton(formatSenderNameFromMsg(msg.getSender())));

    // Formato EXATO do Subject que o Tester quer
    failureNotice.setSubject("FAILED TO SEND " + msg.getId() + " TO " + failedDestination + ": UNKNOWN USER");

    // O Tester espera que o corpo do erro tenha os mesmos conteúdos da original
    failureNotice.setContents(msg.getContents());

    // Assegura que tem tempo de criação
    failureNotice.setCreationTime(System.currentTimeMillis());

    return failureNotice;

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
  private String formatSenderNameFromMsg(String sender) {
    String[] tmp = null;
    if (sender.contains(" ")) {
      tmp = sender.split(" ");
      sender = tmp[tmp.length - 1];

    }
    if (sender.contains("<"))
      return sender.substring(1, sender.length() - 1);
    else
      return sender;
  }

  private boolean checkUserExistenceAndPassword(String name, String pwd) {
    if (name == null) {
      Log.info("Sender Name is none");
      return false;
    } else if (name.equals("<System>")) {
      return true;
    } else if (name.matches(REGEX_LOCAL) && getDomain(name).equals(this.domain)) {

      Users usersClient = getLocalUsersClient();

      int max_retries = 15;

      for (int i = 0; i < max_retries; i++) {
        try {
          Result<User> result = usersClient.getUser(getOnlyTheName(name), pwd);

          if (result.isOK() && result.value() != null) {
            return true;
          }

          Log.info("Password is wrong or sender does not exist.");
          return false;

        } catch (Exception e) {
          // Connection refused, Timeout, etc.
          if (i == max_retries - 1) {
            Log.warning("Rede falhou definitivamente após " + max_retries + " tentativas: " + e.getMessage());
            break; // Sai do ciclo, vai atirar o SERVICE_UNAVAILABLE lá em baixo
          } else {
            long sleepTime = 200 + (i * 50);
            try {
              Thread.sleep(sleepTime);
            } catch (InterruptedException ignored) {
            }
          }
        }
      }

      throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);

    } else if (name.matches(REGEX_FORWARDING) && !getDomain(name).equals(this.domain)) {
      return true;
    }

    Log.info("Password is wrong or sender does not exist.");
    return false;
  }

  private Message checkIfMessageExist(String msgId, String userName) {
    UserInbox usrInbox = hibernate.get(UserInbox.class, userName);
    if (usrInbox == null) {
      return null;
    }
    return usrInbox.checkIfMessageInInbox(msgId);
  }

  //
  private synchronized Messages getExternalClient(String domain) {
    if (externalClients.containsKey(domain)) {
      return externalClients.get(domain);
    }

    try {
      // 1. Tentar encontrar a Gateway primeiro (Essencial para testes de Proxy)
      Result<URI[]> uris = d.knownUrisOf("Gateway", domain, 1);

      // Se não houver Gateway, procura diretamente o servidor de Mensagens
      if (!uris.isOK() || uris.value().length == 0) {
        uris = d.knownUrisOf("Messages", domain, 1);
      }

      if (uris.isOK() && uris.value().length > 0) {
        URI uri = uris.value()[0];
        Messages msgClient;

        // 2. Lógica Híbrida
        if (uri.getScheme().equalsIgnoreCase("http")) {
          msgClient = new RestMessagesClient(uri);
        } else if (uri.getScheme().equalsIgnoreCase("grpc")) {
          msgClient = new GrpcMessagesClient(uri);
        } else {
          throw new RuntimeException("Unknown URI scheme: " + uri.getScheme());
        }

        externalClients.put(domain, msgClient);
        return msgClient;
      }

      Log.warning("Gateway and Messages server are unavailable for domain " + domain);
      throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);

    } catch (WebApplicationException w) {
      throw w; // Propaga os nossos próprios erros HTTP
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }
  }

  private synchronized Users getLocalUsersClient() {
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
      URI uri = uris.value()[0];
      if (uri.getScheme().equals("grpc")) {
        this.localUsersClient = new GrpcUsersClient(uri);
      } else {
        this.localUsersClient = new RestUsersClient(uri);
      }

      return this.localUsersClient;

    } catch (WebApplicationException w) {

      throw w;
    } catch (Exception e) {

      e.printStackTrace();
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }
  }

  // Versão melhorada
  private void propagateMessage(String pwd, Message msg, List<String> destinations, boolean choice) {
    String sender = formatSenderNameFromMsg(msg.getSender());
    String cleanSender = getOnlyTheName(sender) + "@" + getDomain(sender);

    Map<String, List<String>> domainsMap = new ConcurrentHashMap();
    for (String dest : destinations) {
      String targetDomain = getDomain(dest);
      domainsMap.computeIfAbsent(targetDomain, k -> new ArrayList<>()).add(dest);
    }

    for (Map.Entry<String, List<String>> entry : domainsMap.entrySet()) {
      String targetDomain = entry.getKey();
      List<String> targetDestinations = entry.getValue();

      if (targetDomain.equals(this.domain)) {
        for (String dest : targetDestinations) {
          if (choice == POSTMESSAGE) {
            assignMessageToUsers(dest, pwd, msg);
          } else {
            deleteMessageFromUsers(dest, pwd, msg);
          }
        }
        continue; // Terminou o tratamento deste domínio local, avança para o próximo
      }

      final long startTime = System.currentTimeMillis();

      // Tentativa síncrona
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
            // Ignoramos erros de rede, o timeout trata disso
          }
        });

        fastThread.start();
        fastThread.join(2000); // Esperamos no máximo 2 segundos pela resposta

        if (fastThread.isAlive()) {
          fastThread.interrupt();
          throw new RuntimeException("Timeout of 2s hitted!");
        }

        Result<?> result = syncResult[0];
        if (result != null) {
          if (result.isOK()) {
            continue; // Sucesso para este domínio, passa ao próximo
          } else if (result.error() == ErrorCode.NOT_FOUND ||
              result.error() == ErrorCode.FORBIDDEN ||
              result.error() == ErrorCode.BAD_REQUEST) {

            // Erro definitivo
            if (choice == POSTMESSAGE && !msg.getId().contains(".")) {
              // Geramos uma notificação de erro para cada destino que falhou
              for (String failedDest : targetDestinations) {
                Message notification = generateErrorMessage(failedDest, msg);
                propagateMessage(pwd, notification, java.util.Collections.singletonList(cleanSender), POSTMESSAGE);
              }
            }
            continue;
          }
        } else {
          throw new RuntimeException("Result null");
        }
      } catch (Exception e) {
        Log.info("Tentativa síncrona abortada para " + targetDomain + ". A passar para Thread de Background...");
      }

      // Tentativa assíncrona
      new Thread(() -> {
        long timeoutMillis = 90 * 1000;
        boolean success = false;
        boolean unknownUser = false;

        while (true) {
          long elapsed = System.currentTimeMillis() - startTime;
          if (elapsed >= timeoutMillis)
            break;

          if (timeoutMillis - elapsed < 35000) {
            try {
              long sleepTime = timeoutMillis - elapsed;
              if (sleepTime > 0)
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
            }
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
              break;
            }
          } catch (Exception e) {

          }

          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            break;
          }
        }

        // Timeout atingido na thread de background
        if (!success && choice == POSTMESSAGE && !msg.getId().contains(".")) {
          Log.info("TIMEOUT REACHED (90s): Generating failed notification to " + sender);

          for (String failedDest : targetDestinations) {
            Message notification = generateErrorMessage(failedDest, msg);
            if (!unknownUser) {
              notification.setSubject("FAILED TO SEND " + msg.getId() + " TO " + failedDest + ": TIMEOUT");
            }

            try {
              Messages localClient = getExternalClient(getDomain(cleanSender));
              localClient.postMessage(pwd, notification);
            } catch (Exception e) {
              propagateMessage(pwd, notification, java.util.Collections.singletonList(cleanSender), POSTMESSAGE);
            }
          }
        }
      }).start();

    }
  }

  // Versão melhorada
  private Result<Void> assignMessageToUsers(String des, String pwd, Message msg) {

    synchronized (des.intern()) {
      try {
        UserInbox usrIn = hibernate.get(UserInbox.class, des);

        if (usrIn == null) {
          String senderRaw = formatSenderNameFromMsg(msg.getSender());
          boolean isSystem = "<System>".equals(senderRaw);
          boolean isBounceNotice = msg.getId() != null && msg.getId().contains(".");
          String targetName = getOnlyTheName(des);

          boolean targetExists = false;

          if (isSystem || isBounceNotice) {
            targetExists = true;
          } else {
            Users usersClient = getLocalUsersClient();
            String tempPwd = UUID.randomUUID().toString();
            User probeUser = new User(targetName, tempPwd, "Sonda", this.domain);

            targetExists = false;

            for (int i = 0; i < 5; i++) {
              try {
                Result<String> probeRes = usersClient.postUser(probeUser);

                if (probeRes.isOK()) {
                  targetExists = false;
                  try {
                    usersClient.deleteUser(targetName, tempPwd);
                  } catch (Exception ignored) {
                  }
                  break;
                } else if (probeRes.error() == ErrorCode.CONFLICT) {
                  targetExists = true;
                  break;
                } else if (probeRes.error() == ErrorCode.TIMEOUT || probeRes.error() == ErrorCode.INTERNAL_ERROR) {
                  if (i < 4) {
                    Thread.sleep(200);
                    continue;
                  } else {
                    targetExists = true;
                  }
                } else {
                  targetExists = false;
                  break;
                }
              } catch (Exception e) {
                if (i < 4) {
                  Thread.sleep(200);
                } else {
                  targetExists = true;
                }
              }
            }
          }

          if (!targetExists) {
            Log.info("Destinatário " + des + " não existe (Sonda bloqueou). Rejeitado com NOT_FOUND.");
            return Result.error(ErrorCode.NOT_FOUND);
          }

          usrIn = new UserInbox(des);
          hibernate.persist(usrIn);
        }

        Message obtained_msg_inbox = usrIn.checkIfMessageInInbox(msg.getId());
        if (obtained_msg_inbox == null) {
          usrIn.addMessage(msg);
          hibernate.update(usrIn);
        }

        return Result.ok();

      } catch (ConstraintViolationException e) {
        return Result.error(ErrorCode.CONFLICT);
      } catch (Exception e) {
        Log.severe("Erro interno de Base de Dados: " + e.getMessage());
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
  }

  private Result<Void> deleteMessageFromUsers(String des, String pwd, Message msg) {

    String targetDomain = getDomain(des);
    if (targetDomain.equals(this.domain)) {
      UserInbox usrIn = null;
      // See if the mailbox user exists
      try {
        usrIn = hibernate.get(UserInbox.class, des);

        if (usrIn != null) {
          Message cur_message = usrIn.checkIfMessageInInbox(msg.getId());
          if (cur_message != null) {
            usrIn.removeMessage(msg.getId());
            hibernate.update(usrIn);
          }
          // If the mailbox exists but there is no message there with that id it's ok also

        } // If it doesnt have a mailbox it is ok because the message doesnt exist
        return Result.ok();

      } catch (Exception e) {
        e.printStackTrace();
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
    Messages msgClient = getExternalClient(targetDomain);

    try {
      Result<Void> result = msgClient.deleteMessage(des, msg.getId(), pwd);
      return result;
    } catch (Exception e) {
      e.printStackTrace();
      // We dont know the cause of the failure so we cannot give an exact status..
      return Result.error(ErrorCode.TIMEOUT);
    }
  }

  // Versão melhorada
  @Override
  public String postMessage(String pwd, Message msg) {
    if (pwd == null || msg == null) {
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    String sender_name = msg.getSender();
    if (sender_name == null) {
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    boolean isNewMessage = (msg.getId() == null);
    boolean isLocalSender = getDomain(sender_name).equals(this.domain);
    boolean isBounceNotice = msg.getId() != null && msg.getId().contains(".");

    if (isLocalSender && !isBounceNotice) {
      if (!checkUserExistenceAndPassword(sender_name, pwd)) {
        throw new WebApplicationException(Status.FORBIDDEN);
      }
    }

    if (isNewMessage) {
      if (!isLocalSender) {
        throw new WebApplicationException(Status.BAD_REQUEST);
      }
      Users usersClient = getLocalUsersClient();
      Result<User> result = usersClient.getUser(getOnlyTheName(sender_name), pwd);
      if (!result.isOK()) {
        throw new WebApplicationException(Status.FORBIDDEN);
      }
      msg.setId(UUID.randomUUID().toString());
      msg.setSender(result.value().getDisplayName() + " <" + sender_name + ">");
      msg.setCreationTime(System.currentTimeMillis());
      sender_name = msg.getSender();
    }

    String rawSenderEmail = formatSenderNameFromMsg(sender_name);
    boolean isOriginatingServer = getDomain(rawSenderEmail).equals(this.domain);

    List<String> remoteDestinations = new ArrayList<>();
    List<Message> failureNoticesToPropagate = new ArrayList<>();

    synchronized (msg.getId().intern()) {
      try {
        Message existing = hibernate.get(Message.class, msg.getId());
        if (existing == null) {
          hibernate.persist(msg);
        } else {
          if ("<Deleted>".equals(existing.getSender())) {
            Log.info("O POST chegou depois do DELETE. A ignorar mensagem!");
            return msg.getId();
          }
          hibernate.update(msg);
        }
      } catch (Exception e) {
        Log.severe("Error persisting message: " + e.getMessage());
        throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
      }

      for (String destination : msg.getDestination()) {
        if (checkIfUserIsFromDomain(destination)) {
          Result<Void> res = assignMessageToUsers(destination, pwd, msg);
          if (!res.isOK() && res.error() == ErrorCode.NOT_FOUND) {
            Message failureNotice = generateErrorMessage(destination, msg);
            try {
              if (hibernate.get(Message.class, failureNotice.getId()) == null) {
                hibernate.persist(failureNotice);
              }
            } catch (Exception e) {
            }
            if (!msg.getId().contains(".")) {
              failureNoticesToPropagate.add(failureNotice);
            }
          }
        }

        else {
          if (isOriginatingServer || msg.getId().contains(".")) {
            remoteDestinations.add(destination);
          }
        }
      }
    }

    if (!remoteDestinations.isEmpty()) {
      propagateMessage(pwd, msg, remoteDestinations, POSTMESSAGE);
    }

    for (Message failureNotice : failureNoticesToPropagate) {
      propagateMessage(pwd, failureNotice, Collections.singletonList(rawSenderEmail), POSTMESSAGE);
    }

    return msg.getId();
  }

  @Override
  public Message getMessage(String name, String mid, String pwd) {
    Log.info("RECEBI PEDIDO: user=" + name + ", mid=" + mid + ", pwd=" + pwd);
    if (name == null || mid == null || pwd == null) {
      Log.info("Password or Message null");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }
    String fullName = name.contains("@") ? name : name + "@" + this.domain;
    if (!checkUserExistenceAndPassword(fullName, pwd)) {
      Log.info("User doesn't exist or password is wrong");
      throw new WebApplicationException(Status.FORBIDDEN);
    }
    // 1ª Tentativa de leitura
    Message msg = checkIfMessageExist(mid, fullName);

    if (msg == null) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        // Ignora
      }

      // 2ª Tentativa de leitura
      msg = checkIfMessageExist(mid, fullName);
    }

    // Se a mensagem continuar a não existir após o compasso de espera
    if (msg == null) {
      Log.info("The message doesn't exist (even after waiting 500ms).");
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
    String fullName = name.contains("@") ? name : name + "@" + this.domain;
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

  @Override
  public void removeFromUserInbox(String name, String mid, String pwd) {

    Log.info("removeFromUserInbox : name = " + name + "; mid = " + mid + "; pwd = " + pwd);
    if (name == null || mid == null || pwd == null) {
      Log.info("Params can be null.");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    String fullName = name.contains("@") ? name : name + "@" + this.domain;

    // Check if the password is right and the user exists
    if (!checkUserExistenceAndPassword(fullName, pwd)) {
      Log.info("User doesn't exist or password is wrong");
      throw new WebApplicationException(Status.FORBIDDEN);
    }

    try {
      // Get the inBox of the user
      UserInbox inbox = hibernate.get(UserInbox.class, fullName);

      // If the inBox doesn't exist, or is empty, the message isn't there
      if (inbox == null || inbox.getMessages().isEmpty()) {
        Log.info("The mail box is Empty or doesn't exist");
        throw new WebApplicationException(Status.NOT_FOUND);
      }

      Message msg = inbox.checkIfMessageInInbox(mid);
      if (msg == null) {
        Log.info("Message " + mid + " wasn't found in the box " + name);
        throw new WebApplicationException(Status.NOT_FOUND);
      }

      inbox.removeMessage(mid);

      hibernate.update(inbox);
      Log.info("Message " + mid + " removed with success!");

    } catch (WebApplicationException w) {
      throw w;
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
    String fullName = name.contains("@") ? name : name + "@" + this.domain;
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

    if (inbox == null) {
      return new ArrayList<>();
    }

    return inbox.getMessages(query);
  }

  // Versão melhorada
  @Override
  public void deleteMessage(String name, String mid, String pwd) {
    if (name == null || mid == null || pwd == null) {
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    String fullName = name.contains("@") ? name : name + "@" + this.domain;
    List<String> remoteDestinations = new ArrayList<>();
    Message msgToPropagate = null;

    synchronized (mid.intern()) {
      Message msg = hibernate.get(Message.class, mid);

      if (msg == null) {
        Message tombstone = new Message();
        tombstone.setId(mid);
        tombstone.setSender("<Deleted>");
        tombstone.setCreationTime(0L);
        try {
          hibernate.persist(tombstone);
        } catch (Exception e) {
        }
        return;
      }

      if ("<Deleted>".equals(msg.getSender())) {
        return;
      }

      String rawSender = formatSenderNameFromMsg(msg.getSender());
      boolean isOriginatingServer = getDomain(rawSender).equals(this.domain);

      if (isOriginatingServer) {
        if (!checkUserExistenceAndPassword(fullName, pwd)) {
          throw new WebApplicationException(Status.FORBIDDEN);
        }
        if (!rawSender.equals(fullName)) {
          throw new WebApplicationException(Status.FORBIDDEN);
        }
      } else {
        if (!rawSender.equals(fullName)) {
          throw new WebApplicationException(Status.FORBIDDEN);
        }
      }

      for (String destination : msg.getDestination()) {
        if (checkIfUserIsFromDomain(destination)) {
          UserInbox inbox = hibernate.get(UserInbox.class, destination);
          if (inbox != null) {
            inbox.removeMessage(mid);
            hibernate.update(inbox);
          }
        } else if (isOriginatingServer) {
          remoteDestinations.add(destination);
        }
      }

      try {
        hibernate.delete(msg);
      } catch (Exception e) {
      }
      msgToPropagate = msg;
    }

    if (!remoteDestinations.isEmpty() && msgToPropagate != null) {
      propagateMessage(pwd, msgToPropagate, remoteDestinations, DELETEMESSAGE);
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