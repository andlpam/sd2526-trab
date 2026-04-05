package sd2526.trab.server.resources;

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
    // failureNotice.setSender("<System>");
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
      // Message needs to be sent by this server case
    } else if (name.equals("<System>")) {
      return true;
    } else if (name.matches(REGEX_LOCAL) && getDomain(name).equals(this.domain)) {

      // Talk to the server to find the user object of the sender from this domain
      Users usersClient = getLocalUsersClient();
      Result<User> result = usersClient.getUser(getOnlyTheName(name), pwd);

      // If the user is not created yet create the user

      // Check if the sender exists in the users server
      if (result.isOK()) {
        Log.info("Found " + result.value().getName());
        return true;
      }
      // The receiving server of the message so we dont need to verify neither the
      // password nor the user
    } else if (name.matches(REGEX_FORWARDING) && !getDomain(name).equals(this.domain)) {
      return true;
    }
    Log.info("Password is wrong or sender does not exist.");
    return false;
    // Assign the ID
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
      Result<URI[]> uris = d.knownUrisOf("Gateway", domain, 1);
      if (uris.isOK()) {
        Log.info("Found Gateway for domain " + domain);
        Messages gtwClient = new RestGatewayClient(uris.value()[0]);
        externalClients.putIfAbsent(domain, gtwClient);
        return gtwClient;
      }

      Log.info("No Gateway found for " + domain + ". Falling back to Messages server.");
      uris = d.knownUrisOf("Messages", domain, 1);
      if (uris.isOK()) {
        Messages msgClient = new RestMessagesClient(uris.value()[0]);
        externalClients.putIfAbsent(domain, msgClient);
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
      localUsersClient = new RestUsersClient(uris.value()[0]);
      return localUsersClient;

    } catch (WebApplicationException w) {

      throw w;
    } catch (Exception e) {

      e.printStackTrace();
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }
  }

  /*
   * private void propagateMessage(String pwd, Message msg, String
   * destination_address, boolean choice) {
   * String sender = formatSenderNameFromMsg(msg.getSender());
   * String targetDomain = getDomain(destination_address);
   * 
   * // Se o destino for o nosso próprio domínio (acontece quando enviamos as
   * // notificações de erro de volta),
   * // não precisamos de ir à rede, resolvemos logo!
   * if (targetDomain.equals(this.domain)) {
   * if (choice == POSTMESSAGE) {
   * assignMessageToUsers(destination_address, pwd, msg);
   * } else {
   * deleteMessageFromUsers(destination_address, pwd, msg);
   * }
   * return;
   * }
   * 
   * // Se for para fora, lançamos a Thread
   * new Thread(() -> {
   * long startTime = System.currentTimeMillis();
   * long timeoutMillis = 90 * 1000;
   * boolean success = false;
   * boolean unknownUser = false;
   * 
   * while ((System.currentTimeMillis() - startTime) < timeoutMillis) {
   * try {
   * Messages msgClient = getExternalClient(targetDomain);
   * Result<?> result;
   * 
   * if (choice == POSTMESSAGE) {
   * result = msgClient.postMessage(pwd, msg);
   * } else {
   * // result = msgClient.deleteMessage(destination_address, msg.getId(), pwd);
   * String cleanSender = getOnlyTheName(sender) + "@" + getDomain(sender);
   * result = msgClient.deleteMessage(cleanSender, msg.getId(), pwd);
   * }
   * 
   * if (result.isOK()) {
   * success = true;
   * break;
   * }
   * // Se for erro definitivo do outro lado (User não existe, Password errada)
   * não
   * // fazemos retry
   * else if (result.error() == ErrorCode.NOT_FOUND ||
   * result.error() == ErrorCode.FORBIDDEN ||
   * result.error() == ErrorCode.BAD_REQUEST) {
   * if (result.error() == ErrorCode.NOT_FOUND)
   * unknownUser = true;
   * break;
   * }
   * 
   * // Se for erro de servidor interno ou timeout, tentamos de novo
   * Thread.sleep(3000);
   * 
   * } catch (Exception e) {
   * try {
   * Thread.sleep(3000);
   * } catch (InterruptedException x) {
   * break;
   * }
   * }
   * }
   * 
   * // Tratamento de Falha (Se não for do <System>)
   * if (!success && !msg.getId().contains(".")) {
   * Log.info("Generating a failed notification to the remetent " + sender);
   * 
   * Message notification = generateErrorMessage(destination_address, msg);
   * if (!unknownUser) {
   * notification.setSubject("FAILED TO SEND " + msg.getId() + " TO " +
   * destination_address + ": TIMEOUT");
   * }
   * 
   * // Envia a notificação para o sender (viaja pela rede ou fica local)
   * String cleanSender = getOnlyTheName(sender) + "@" + getDomain(sender);
   * propagateMessage(pwd, notification, cleanSender, POSTMESSAGE);
   * }
   * 
   * }).start();
   * }
   */

  // Versão melhorada
  private void propagateMessage(String pwd, Message msg, String destination_address, boolean choice) {
    String targetDomain = getDomain(destination_address);

    // Lógica Local
    if (targetDomain.equals(this.domain)) {
      if (choice == POSTMESSAGE) {
        assignMessageToUsers(destination_address, pwd, msg);
      } else {
        // Ajusta para o nome da tua função de apagar local
        deleteMessageFromUsers(destination_address, pwd, msg);
      }
      return;
    }

    final long startTime = System.currentTimeMillis();
    String sender = formatSenderNameFromMsg(msg.getSender());
    String cleanSender = getOnlyTheName(sender) + "@" + getDomain(sender);

    // Tentativa sincrona
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
          return;
        } else if (result.error() == ErrorCode.NOT_FOUND ||
            result.error() == ErrorCode.FORBIDDEN ||
            result.error() == ErrorCode.BAD_REQUEST) {

          if (choice == POSTMESSAGE && !msg.getId().contains(".")) {
            Message notification = generateErrorMessage(destination_address, msg);
            propagateMessage(pwd, notification, cleanSender, POSTMESSAGE);
          }
          return; // Erro definitivo
        }
      } else {
        throw new RuntimeException("Result null");
      }
    } catch (Exception e) {
      Log.info("Tentativa síncrona abortada. A passar para Thread de Background...");
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
          // Rede cortada, ciclo repete
        }

        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          break;
        }
      }

      // Timeout Atingido
      if (!success && choice == POSTMESSAGE && !msg.getId().contains(".")) {
        Log.info("TIMEOUT REACHED (90s): Generating failed notification to " + sender);

        Message notification = generateErrorMessage(destination_address, msg);
        if (!unknownUser) {
          notification.setSubject("FAILED TO SEND " + msg.getId() + " TO " + destination_address + ": TIMEOUT");
        }

        try {
          // Self-Invocation HTTP
          Messages localClient = getExternalClient(getDomain(cleanSender));
          localClient.postMessage(pwd, notification);
        } catch (Exception e) {
          propagateMessage(pwd, notification, cleanSender, POSTMESSAGE);
        }
      }
    }).start();
  }
  /*
   * private Result<Void> assignMessageToUsers(String des, String pwd, Message
   * msg) {
   * try {
   * UserInbox usrIn = hibernate.get(UserInbox.class, des);
   * 
   * if (usrIn == null) {
   * // If the message came from the system then deliver the message
   * if (msg.getId().contains(".")) {
   * usrIn = new UserInbox(des);
   * hibernate.persist(usrIn);
   * } else {
   * // Utilizador não tem Inbox. Vamos ver se ele existe no UsersServer
   * Users usersClient = getLocalUsersClient();
   * String senderForSearch = formatSenderNameFromMsg(msg.getSender());
   * 
   * Result<List<User>> user =
   * usersClient.searchUsers(getOnlyTheName(senderForSearch), pwd,
   * getOnlyTheName(des));
   * if (user.isOK() && user.value() != null && !user.value().isEmpty()) {
   * usrIn = new UserInbox(des);
   * hibernate.persist(usrIn);
   * } else {
   * return Result.error(ErrorCode.NOT_FOUND);
   * }
   * }
   * }
   * 
   * // Adiciona à inbox de forma segura (sem rebentar a BD)
   * Message obtained_msg_inbox = usrIn.checkIfMessageInInbox(msg.getId());
   * if (obtained_msg_inbox == null) {
   * usrIn.addMessage(msg);
   * hibernate.update(usrIn);
   * }
   * return Result.ok();
   * 
   * } catch (ConstraintViolationException e) {
   * Log.info("User entry already exists in the inbox database");
   * return Result.error(ErrorCode.CONFLICT);
   * } catch (Exception e) {
   * Log.severe("Error saving in the database " + e.getMessage());
   * return Result.error(ErrorCode.INTERNAL_ERROR);
   * }
   * }
   */

  // Versão melhorada
  private synchronized Result<Void> assignMessageToUsers(String des, String pwd, Message msg) {
    try {
      UserInbox usrIn = hibernate.get(UserInbox.class, des);

      // Se a caixa não existir
      if (usrIn == null) {
        String senderRaw = formatSenderNameFromMsg(msg.getSender());
        boolean isSystem = "<System>".equals(senderRaw);
        boolean isRemoteSender = !getDomain(senderRaw).equals(this.domain);
        boolean isBounceNotice = msg.getId() != null && msg.getId().contains(".");
        String targetName = getOnlyTheName(des);

        // Assumimos logo de início que o utilizador existe!
        boolean targetExists = true;

        // Se for um envio estritamente local,
        // fazemos a verificação normal com a password real, porque é rápido.
        if (!isRemoteSender && !isSystem && !isBounceNotice) {
          Users usersClient = getLocalUsersClient();
          Result<List<User>> userRes = usersClient.searchUsers(getOnlyTheName(senderRaw), pwd, targetName);

          targetExists = false; // Resetamos para provar que existe

          if (userRes.isOK() && userRes.value() != null) {
            for (User u : userRes.value()) {
              if (u.getName().equals(targetName)) {
                targetExists = true;
                break;
              }
            }
          }
        }

        // Se falhou a verificação estritamente local
        if (!targetExists) {
          return Result.error(ErrorCode.NOT_FOUND);
        }

        // Cria a caixa quase instantaneamente sem chamadas REST fantasmas
        usrIn = new UserInbox(des);
        hibernate.persist(usrIn);
      }

      // Adiciona a mensagem e guarda
      Message obtained_msg_inbox = usrIn.checkIfMessageInInbox(msg.getId());
      if (obtained_msg_inbox == null) {
        usrIn.addMessage(msg);
        hibernate.update(usrIn);
      }

      return Result.ok();

    } catch (ConstraintViolationException e) {
      Log.info("User entry already exists in the inbox database");
      return Result.error(ErrorCode.CONFLICT);
    } catch (Exception e) {
      Log.severe("Error saving in the database " + e.getMessage());
      return Result.error(ErrorCode.INTERNAL_ERROR);
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

  /*
   * @Override
   * public String postMessage(String pwd, Message msg) {
   * if (pwd == null || msg == null) {
   * Log.info("Password or Message null");
   * throw new WebApplicationException(Status.BAD_REQUEST);
   * }
   * Log.info("postMessage : pwd = " + pwd + "; msg = " + msg);
   * 
   * String sender_name = msg.getSender();
   * if (sender_name == null) {
   * throw new WebApplicationException(Status.BAD_REQUEST);
   * }
   * 
   * // Password Validation
   * if (!checkUserExistenceAndPassword(sender_name, pwd)) {
   * Log.info("Password is incorrect or user does not exist.");
   * throw new WebApplicationException(Status.FORBIDDEN);
   * }
   * 
   * // Message Formatation
   * if (msg.getId() == null && getDomain(sender_name).equals(this.domain)) {
   * Users usersClient = getLocalUsersClient();
   * Result<User> result = usersClient.getUser(getOnlyTheName(sender_name), pwd);
   * 
   * if (!result.isOK()) {
   * throw new WebApplicationException(Status.FORBIDDEN);
   * }
   * 
   * msg.setId(UUID.randomUUID().toString());
   * msg.setSender(result.value().getDisplayName() + " <" + sender_name + ">");
   * msg.setCreationTime(System.currentTimeMillis());
   * } else if (msg.getId() == null) {
   * throw new WebApplicationException(Status.BAD_REQUEST); // Se não for daqui,
   * já devia ter ID!
   * }
   * 
   * // Message Persistence
   * try {
   * if (hibernate.get(Message.class, msg.getId()) == null) {
   * hibernate.persist(msg);
   * } else {
   * hibernate.update(msg);
   * }
   * } catch (Exception e) {
   * Log.severe("Error persisting message: " + e.getMessage());
   * throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
   * }
   * 
   * // Distribution to the destinations
   * for (String destination : msg.getDestination()) {
   * if (checkIfUserIsFromDomain(destination)) {
   * 
   * Result<Void> res = assignMessageToUsers(destination, pwd, msg);
   * 
   * // If it fails the local user does not exist, warn the Sender!
   * if (!res.isOK() && res.error() == ErrorCode.NOT_FOUND) {
   * Log.info("Failure Notification: " + destination + " not found.");
   * Message failureNotice = generateErrorMessage(destination, msg);
   * // Save the message locally
   * try {
   * hibernate.persist(failureNotice);
   * } catch (ConstraintViolationException e) {
   * e.printStackTrace();
   * } catch (Exception x) {
   * x.printStackTrace();
   * }
   * 
   * String cleanSender = getOnlyTheName(sender_name) + "@" +
   * getDomain(sender_name);
   * 
   * // If it is a <System> it is already an error
   * if (!sender_name.equals("<System>")) {
   * propagateMessage(pwd, failureNotice, cleanSender, POSTMESSAGE);
   * }
   * }
   * } else {
   * //
   * propagateMessage(pwd, msg, destination, POSTMESSAGE);
   * }
   * }
   * 
   * return msg.getId();
   * }
   */

  // Versão melhorada
  @Override
  public String postMessage(String pwd, Message msg) {
    if (pwd == null || msg == null) {
      Log.info("Password or Message null");
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    String sender_name = msg.getSender();
    if (sender_name == null) {
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    Log.info("postMessage : pwd = " + pwd + "; msg = " + msg);

    boolean isNewMessage = (msg.getId() == null);
    boolean isLocalSender = getDomain(sender_name).equals(this.domain);
    boolean isBounceNotice = msg.getId() != null && msg.getId().contains(".");

    // Se for uma mensagem do nosso domínio e não for um aviso de erro validamos a
    // password
    if (isLocalSender && !isBounceNotice) {
      if (!checkUserExistenceAndPassword(sender_name, pwd)) {
        Log.info("Password is incorrect or user does not exist.");
        throw new WebApplicationException(Status.FORBIDDEN);
      }
    }

    // Se é uma mensagem nova
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

    } else if (!isNewMessage && isLocalSender && !msg.getId().contains(".")) {
      // Se já tem ID, mas não é um aviso de erro
      Log.info("Message already has ID but claims to be local. Skipping auth as it might be a bounce.");
    }

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

    // Usamos o formato email@domain para fazer verificações
    String rawSenderEmail = formatSenderNameFromMsg(sender_name);
    boolean isOriginatingServer = getDomain(rawSenderEmail).equals(this.domain);

    // Distribuição
    Set<String> propagatedDomains = new HashSet<>();
    // Lista para guardarmos os avisos de falha que temos de enviar de volta
    List<Message> failureNoticesToPropagate = new ArrayList<>();

    for (String destination : msg.getDestination()) {

      // Se o destino for o nosso servidor local
      if (checkIfUserIsFromDomain(destination)) {
        Result<Void> res = assignMessageToUsers(destination, pwd, msg);

        // Se falhar porque o utilizador local não existe, avisamos o remetente original
        if (!res.isOK() && res.error() == ErrorCode.NOT_FOUND) {
          Log.info("Failure Notification: " + destination + " not found.");
          Message failureNotice = generateErrorMessage(destination, msg);

          try {
            if (hibernate.get(Message.class, failureNotice.getId()) == null) {
              hibernate.persist(failureNotice);
            }
          } catch (Exception e) {
            Log.warning("Could not persist failure notice: " + e.getMessage());
          }

          // Se a mensagem original não for um erro
          if (!msg.getId().contains(".")) {
            // Em vez de mandarmos já pela rede e encravar o ciclo, pomos na lista
            failureNoticesToPropagate.add(failureNotice);
          }
        }
      }
      // Se o destino for para outro domínio
      else {
        if (isOriginatingServer || msg.getId().contains(".")) {
          String targetDomain = getDomain(destination);

          if (!propagatedDomains.contains(targetDomain)) {
            propagatedDomains.add(targetDomain);
            propagateMessage(pwd, msg, destination, POSTMESSAGE);
          }
        }
      }
    }

    // Enviar as notificações de falha geradas durante o ciclo
    for (Message failureNotice : failureNoticesToPropagate) {
      propagateMessage(pwd, failureNotice, rawSenderEmail, POSTMESSAGE);
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
        // Dá 500 milissegundos de avanço para as Threads do POST
        // terminarem de criar os fantasmas e fazerem o COMMIT na Base de Dados
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

  // 1.First we need to check if the user is the user exists in the server
  // 2.Second we need to go to the user inbox and just remove it
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
      inbox.removeMessage(mid);
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

    return inbox.getMessages(query);
  }

  /*
   * @Override
   * public void deleteMessage(String name, String mid, String pwd) {
   * // 1. Validação de parâmetros nulos
   * if (name == null || mid == null || pwd == null) {
   * Log.info("Invalid parameters for deleteMessage");
   * throw new WebApplicationException(Status.BAD_REQUEST);
   * }
   * 
   * // 2. Autenticação: Verificar se o utilizador existe e a password está
   * correta
   * String fullName = name.contains("@") ? name : name + "@" + this.domain;
   * if (!checkUserExistenceAndPassword(fullName, pwd)) {
   * Log.info("Authentication failed for user: " + name);
   * throw new WebApplicationException(Status.FORBIDDEN);
   * }
   * 
   * try {
   * 
   * Message cur_message = hibernate.get(Message.class, mid);
   * 
   * if (cur_message == null) {
   * Log.info("Message " + mid + " not found. Skipping deletion.");
   * return;
   * }
   * 
   * // Authentication
   * if (cur_message.getSender() == null ||
   * !cur_message.getSender().contains(name)) {
   * Log.info("Authorization failed: user " + name + " is not the sender.");
   * throw new WebApplicationException(Status.FORBIDDEN);
   * }
   * 
   * // Rule of 30 seconds to delete
   * long messageAge = System.currentTimeMillis() - cur_message.getCreationTime();
   * if (messageAge > DELETE_MESSAGE_TIME) {
   * Log.info("Message is too old to be deleted: " + (messageAge / 1000) + "s");
   * // A especificação diz que só deve ser apagada se tiver menos de 30s.
   * // Se for mais velha, simplesmente ignoramos e retornamos sucesso
   * (NO_CONTENT).
   * return;
   * }
   * 
   * // Remove inboxes
   * Set<String> destinations = cur_message.getDestination();
   * for (String des : destinations) {
   * if (checkIfUserIsFromDomain(des)) {
   * // Remoção local direta na base de dados (através do UserInbox)
   * Result<Void> res = deleteMessageFromUsers(des, pwd, cur_message);
   * if (!res.isOK()) {
   * Log.warning("Failed to delete local message reference for: " + des);
   * throw new
   * WebApplicationException(GatewayResource.getStatusFromErrorCode(res.error()));
   * }
   * } else {
   * 
   * propagateMessage(pwd, cur_message, des, DELETEMESSAGE);
   * }
   * }
   * 
   * } catch (WebApplicationException w) {
   * throw w;
   * } catch (Exception e) {
   * Log.severe("Unexpected error in deleteMessage: " + e.getMessage());
   * e.printStackTrace();
   * throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
   * }
   * }
   */

  // Versão melhorada
  @Override
  public void deleteMessage(String name, String mid, String pwd) {
    if (name == null || mid == null || pwd == null) {
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    Message msg = hibernate.get(Message.class, mid);
    if (msg == null) {
      // Se a mensagem não existe, pode ser o DELETE a chegar antes do POST.
      // Criamos uma Message para avisar o POST de que ele já está morto
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

    String rawSender = formatSenderNameFromMsg(msg.getSender());
    boolean isOriginatingServer = getDomain(rawSender).equals(this.domain);
    String fullName = name.contains("@") ? name : name + "@" + this.domain;

    if (isOriginatingServer) {
      // É uma mensagem do domínio local, temos de validar a password
      if (!checkUserExistenceAndPassword(fullName, pwd)) {
        throw new WebApplicationException(Status.FORBIDDEN);
      }
      // Só o remetente pode apagar
      if (!rawSender.equals(fullName)) {
        throw new WebApplicationException(Status.FORBIDDEN);
      }
    } else {
      // Confiamos no servidor de origem e saltamos o check da password.
      // Mas confirmamos que o pedido vem em nome do verdadeiro remetente
      if (!rawSender.equals(fullName)) {
        throw new WebApplicationException(Status.FORBIDDEN);
      }
    }

    // Remover das caixas locais e propagar
    for (String destination : msg.getDestination()) {
      if (checkIfUserIsFromDomain(destination)) {
        UserInbox inbox = hibernate.get(UserInbox.class, destination);
        if (inbox != null) {
          inbox.removeMessage(mid);
          hibernate.update(inbox);
        }
      } else if (isOriginatingServer) {
        // Só propagamos a eliminação para a rede se nós formos a origem
        propagateMessage(pwd, msg, destination, DELETEMESSAGE);
      }
    }

    // Remover a mensagem principal da BD
    try {
      hibernate.delete(msg);
    } catch (Exception e) {
      Log.warning("Message already deleted or error: " + e.getMessage());
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