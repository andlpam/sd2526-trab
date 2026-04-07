package sd2526.trab.server.grpc.controllers;

import com.google.protobuf.Empty;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.api.UserInbox;
import sd2526.trab.api.grpc.GrpcMessagesGrpc;
import sd2526.trab.api.grpc.Messages.*;
import sd2526.trab.discovery.Discovery;
import sd2526.trab.server.grpc.utils.DataModelAdaptor;
import sd2526.trab.server.persistence.Hibernate;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.client.rest.RestMessagesClient;
// Assume que terás um GrpcMessagesClient para falar com outros domínios gRPC
import sd2526.trab.client.grpc.GrpcMessagesClient;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Users;
import sd2526.trab.client.rest.RestUsersClient;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class GrpcMessagesController implements GrpcMessagesGrpc.AsyncService, BindableService {
  private static Logger Log = Logger.getLogger(GrpcMessagesController.class.getName());
  private Hibernate hibernate;
  private final Discovery d;
  private final String domain;

  private Map<String, Messages> externalClients;
  private Users localUsersClient = null;

  private static final boolean POSTMESSAGE = true;
  private static final boolean DELETEMESSAGE = false;
  private static final String REGEX_LOCAL = "^[^@<>\\s]+@[^@<>\\s]+$";
  private static final String REGEX_FORWARDING = "^.+\\s<[^@<>\\s]+@[^@<>\\s]+>$";

  public GrpcMessagesController(String domain, Discovery discovery) {
    this.domain = domain;
    this.d = discovery;
    this.externalClients = new ConcurrentHashMap<>();
  }

  @Override
  public final ServerServiceDefinition bindService() {
    this.hibernate = Hibernate.getInstance();
    return GrpcMessagesGrpc.bindService(this);
  }

  @Override
  public void postMessage(PostMessageArgs request, StreamObserver<PostMessageResult> responseObserver) {
    try {
      Message msg = DataModelAdaptor.GrpcMessage_to_Message(request.getMessage());
      String pwd = request.getPwd();
      String sender_name = msg.getSender();

      if (pwd == null || msg == null || sender_name == null) {
        responseObserver.onError(Status.INVALID_ARGUMENT.asException());
        return;
      }

      boolean isNewMessage = (msg.getId() == null || msg.getId().isEmpty());
      boolean isLocalSender = getDomain(sender_name).equals(this.domain);
      boolean isBounceNotice = msg.getId() != null && msg.getId().contains(".");

      if (isLocalSender && !isBounceNotice) {
        if (!checkUserExistenceAndPassword(sender_name, pwd)) {
          responseObserver.onError(Status.PERMISSION_DENIED.asException());
          return;
        }
      }

      if (isNewMessage) {
        if (!isLocalSender) {
          responseObserver.onError(Status.INVALID_ARGUMENT.asException());
          return;
        }
        Users usersClient = getLocalUsersClient();
        Result<User> result = usersClient.getUser(getOnlyTheName(sender_name), pwd);
        if (!result.isOK()) {
          responseObserver.onError(Status.PERMISSION_DENIED.asException());
          return;
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
        Message existing = hibernate.get(Message.class, msg.getId());
        if (existing == null) {
          hibernate.persist(msg);
        } else {
          if ("<Deleted>".equals(existing.getSender())) {
            Log.info("O POST chegou depois do DELETE. A ignorar mensagem!");
            responseObserver.onNext(PostMessageResult.newBuilder().setMid(msg.getId()).build());
            responseObserver.onCompleted();
            return;
          }
          hibernate.update(msg);
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
          } else {
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

      responseObserver.onNext(PostMessageResult.newBuilder().setMid(msg.getId()).build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void getInboxMessage(GetInboxMessageArgs request, StreamObserver<GrpcMessage> responseObserver) {
    String fullName = request.getName().contains("@") ? request.getName() : request.getName() + "@" + this.domain;
    if (!checkUserExistenceAndPassword(fullName, request.getPwd())) {
      responseObserver.onError(Status.PERMISSION_DENIED.asException());
      return;
    }

    Message msg = checkIfMessageExist(request.getMid(), fullName);
    if (msg == null) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ignored) {
      }
      msg = checkIfMessageExist(request.getMid(), fullName);
    }

    if (msg != null) {
      responseObserver.onNext(DataModelAdaptor.Message_to_GrpcMessage(msg));
      responseObserver.onCompleted();
    } else {
      responseObserver.onError(Status.NOT_FOUND.asException());
    }
  }

  @Override
  public void getAllInboxMessages(GetAllInboxMessagesArgs request,
      StreamObserver<GetAllInboxMessagesResult> responseObserver) {
    String fullName = request.getName().contains("@") ? request.getName() : request.getName() + "@" + this.domain;
    if (!checkUserExistenceAndPassword(fullName, request.getPwd())) {
      responseObserver.onError(Status.PERMISSION_DENIED.asException());
      return;
    }

    UserInbox inbox = hibernate.get(UserInbox.class, fullName);
    GetAllInboxMessagesResult.Builder builder = GetAllInboxMessagesResult.newBuilder();

    if (inbox != null && inbox.getMessages() != null) {
      for (Message m : inbox.getMessages()) {
        builder.addMids(m.getId());
      }
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void removeInboxMessage(RemoveInboxMessageArgs request, StreamObserver<Empty> responseObserver) {
    String fullName = request.getName().contains("@") ? request.getName() : request.getName() + "@" + this.domain;
    if (!checkUserExistenceAndPassword(fullName, request.getPwd())) {
      responseObserver.onError(Status.PERMISSION_DENIED.asException());
      return;
    }

    try {
      UserInbox inbox = hibernate.get(UserInbox.class, fullName);
      if (inbox == null || inbox.getMessages().isEmpty() || inbox.checkIfMessageInInbox(request.getMid()) == null) {
        responseObserver.onError(Status.NOT_FOUND.asException());
        return;
      }

      inbox.removeMessage(request.getMid());
      hibernate.update(inbox);
      responseObserver.onNext(Empty.newBuilder().build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asException());
    }
  }

  @Override
  public void deleteMessage(DeleteMessageArgs request, StreamObserver<Empty> responseObserver) {
    String fullName = request.getName().contains("@") ? request.getName() : request.getName() + "@" + this.domain;
    String mid = request.getMid();
    String pwd = request.getPwd();

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
        } catch (Exception ignored) {
        }
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
        return;
      }

      if ("<Deleted>".equals(msg.getSender())) {
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
        return;
      }

      String rawSender = formatSenderNameFromMsg(msg.getSender());
      boolean isOriginatingServer = getDomain(rawSender).equals(this.domain);

      if (isOriginatingServer) {
        if (!checkUserExistenceAndPassword(fullName, pwd) || !rawSender.equals(fullName)) {
          responseObserver.onError(Status.PERMISSION_DENIED.asException());
          return;
        }
      } else {
        if (!rawSender.equals(fullName)) {
          responseObserver.onError(Status.PERMISSION_DENIED.asException());
          return;
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
      } catch (Exception ignored) {
      }
      msgToPropagate = msg;
    }

    if (!remoteDestinations.isEmpty() && msgToPropagate != null) {
      propagateMessage(pwd, msgToPropagate, remoteDestinations, DELETEMESSAGE);
    }

    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void searchInbox(SearchInboxArgs request, StreamObserver<SearchInboxResult> responseObserver) {
    String fullName = request.getName().contains("@") ? request.getName() : request.getName() + "@" + this.domain;
    if (!checkUserExistenceAndPassword(fullName, request.getPwd())) {
      responseObserver.onError(Status.PERMISSION_DENIED.asException());
      return;
    }

    UserInbox inbox = hibernate.get(UserInbox.class, fullName);
    SearchInboxResult.Builder builder = SearchInboxResult.newBuilder();

    if (inbox != null && request.getQuery() != null && !request.getQuery().isEmpty()) {
      List<String> filtered = inbox.getMessages(request.getQuery());
      if (filtered != null) {
        builder.addAllMids(filtered);
      }
    } else if (inbox != null) {
      for (Message m : inbox.getMessages()) {
        builder.addMids(m.getId());
      }
    }

    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  private String getDomain(String address) {
    address = formatSenderNameFromMsg(address);
    return address.substring(address.lastIndexOf("@") + 1);
  }

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

  private Message checkIfMessageExist(String msgId, String userName) {
    UserInbox usrInbox = hibernate.get(UserInbox.class, userName);
    return (usrInbox == null) ? null : usrInbox.checkIfMessageInInbox(msgId);
  }

  private Message generateErrorMessage(String failedDestination, Message msg) {
    Message failureNotice = new Message();
    String failureId = msg.getId() + "." + failedDestination;
    failureNotice.setId(failureId);
    failureNotice.setSender(msg.getSender());
    failureNotice.setDestination(Collections.singleton(formatSenderNameFromMsg(msg.getSender())));
    failureNotice.setSubject("FAILED TO SEND " + msg.getId() + " TO " + failedDestination + ": UNKNOWN USER");
    failureNotice.setContents(msg.getContents());
    failureNotice.setCreationTime(System.currentTimeMillis());
    return failureNotice;
  }

  private synchronized Users getLocalUsersClient() {
    if (localUsersClient != null)
      return localUsersClient;

    try {
      Result<URI[]> uris = d.knownUrisOf("Users", this.domain, 1);
      if (!uris.isOK())
        throw new RuntimeException("Server is not being able to delivering uris by multicast.");

      // Assume sempre cliente REST local para os Users (ajusta se for necessário
      // gRPC)
      localUsersClient = new RestUsersClient(uris.value()[0]);
      return localUsersClient;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean checkUserExistenceAndPassword(String name, String pwd) {
    if (name == null)
      return false;
    else if (name.equals("<System>"))
      return true;
    else if (name.matches(REGEX_LOCAL) && getDomain(name).equals(this.domain)) {
      Users usersClient = getLocalUsersClient();
      int max_retries = 15;
      for (int i = 0; i < max_retries; i++) {
        try {
          Result<User> result = usersClient.getUser(getOnlyTheName(name), pwd);
          if (result.isOK() && result.value() != null)
            return true;
          return false;
        } catch (Exception e) {
          if (i == max_retries - 1)
            break;
          try {
            Thread.sleep(200 + (i * 50));
          } catch (InterruptedException ignored) {
          }
        }
      }
      throw new RuntimeException("Service Unavailable");
    } else if (name.matches(REGEX_FORWARDING) && !getDomain(name).equals(this.domain)) {
      return true;
    }
    return false;
  }

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
                  } else
                    targetExists = true;
                } else {
                  targetExists = false;
                  break;
                }
              } catch (Exception e) {
                if (i < 4)
                  try {
                    Thread.sleep(200);
                  } catch (Exception ex) {
                  }
                else
                  targetExists = true;
              }
            }
          }

          if (!targetExists)
            return Result.error(ErrorCode.NOT_FOUND);

          usrIn = new UserInbox(des);
          hibernate.persist(usrIn);
        }

        if (usrIn.checkIfMessageInInbox(msg.getId()) == null) {
          usrIn.addMessage(msg);
          hibernate.update(usrIn);
        }
        return Result.ok();

      } catch (Exception e) {
        return Result.error(ErrorCode.INTERNAL_ERROR);
      }
    }
  }

  private Result<Void> deleteMessageFromUsers(String des, String pwd, Message msg) {
    try {
      UserInbox usrIn = hibernate.get(UserInbox.class, des);
      if (usrIn != null) {
        if (usrIn.checkIfMessageInInbox(msg.getId()) != null) {
          usrIn.removeMessage(msg.getId());
          hibernate.update(usrIn);
        }
      }
      return Result.ok();
    } catch (Exception e) {
      return Result.error(ErrorCode.INTERNAL_ERROR);
    }
  }

  private synchronized Messages getExternalClient(String domain) {
    if (externalClients.containsKey(domain)) {
      return externalClients.get(domain);
    }

    try {
      // 1. Tentar encontrar a Gateway primeiro
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
      throw new RuntimeException("Service Unavailable");

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Internal Server Error: " + e.getMessage());
    }
  }

  private void propagateMessage(String pwd, Message msg, List<String> destinations, boolean choice) {
    String sender = formatSenderNameFromMsg(msg.getSender());
    String cleanSender = getOnlyTheName(sender) + "@" + getDomain(sender);

    Map<String, List<String>> domainsMap = new ConcurrentHashMap<>();
    for (String dest : destinations) {
      String targetDomain = getDomain(dest);
      domainsMap.computeIfAbsent(targetDomain, k -> new ArrayList<>()).add(dest);
    }

    for (Map.Entry<String, List<String>> entry : domainsMap.entrySet()) {
      String targetDomain = entry.getKey();
      List<String> targetDestinations = entry.getValue();

      if (targetDomain.equals(this.domain)) {
        for (String dest : targetDestinations) {
          if (choice == POSTMESSAGE)
            assignMessageToUsers(dest, pwd, msg);
          else
            deleteMessageFromUsers(dest, pwd, msg);
        }
        continue;
      }

      final long startTime = System.currentTimeMillis();

      try {
        Messages msgClient = getExternalClient(targetDomain);
        final Result<?>[] syncResult = new Result<?>[1];

        Thread fastThread = new Thread(() -> {
          try {
            if (choice == POSTMESSAGE)
              syncResult[0] = msgClient.postMessage(pwd, msg);
            else
              syncResult[0] = msgClient.deleteMessage(cleanSender, msg.getId(), pwd);
          } catch (Exception ignored) {
          }
        });

        fastThread.start();
        fastThread.join(2000);

        if (fastThread.isAlive()) {
          fastThread.interrupt();
          throw new RuntimeException("Timeout");
        }

        if (syncResult[0] != null) {
          Result<?> result = syncResult[0];
          if (result.isOK())
            continue;
          else if (result.error() == ErrorCode.NOT_FOUND || result.error() == ErrorCode.FORBIDDEN
              || result.error() == ErrorCode.BAD_REQUEST) {
            if (choice == POSTMESSAGE && !msg.getId().contains(".")) {
              for (String failedDest : targetDestinations) {
                Message notification = generateErrorMessage(failedDest, msg);
                propagateMessage(pwd, notification, Collections.singletonList(cleanSender), POSTMESSAGE);
              }
            }
            continue;
          }
        } else {
          throw new RuntimeException("Result null");
        }
      } catch (Exception e) {
        Log.info("Sincrona abortada para " + targetDomain + ". A passar para Background...");
      }

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
            } catch (InterruptedException ignored) {
            }
            break;
          }

          try {
            Messages msgClient = getExternalClient(targetDomain);
            Result<?> result;

            if (choice == POSTMESSAGE)
              result = msgClient.postMessage(pwd, msg);
            else
              result = msgClient.deleteMessage(cleanSender, msg.getId(), pwd);

            if (result.isOK()) {
              success = true;
              break;
            } else if (result.error() == ErrorCode.NOT_FOUND || result.error() == ErrorCode.FORBIDDEN
                || result.error() == ErrorCode.BAD_REQUEST) {
              if (result.error() == ErrorCode.NOT_FOUND)
                unknownUser = true;
              break;
            }
          } catch (Exception ignored) {
          }

          try {
            Thread.sleep(100);
          } catch (InterruptedException ignored) {
            break;
          }
        }

        if (!success && choice == POSTMESSAGE && !msg.getId().contains(".")) {
          for (String failedDest : targetDestinations) {
            Message notification = generateErrorMessage(failedDest, msg);
            if (!unknownUser)
              notification.setSubject("FAILED TO SEND " + msg.getId() + " TO " + failedDest + ": TIMEOUT");

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
}