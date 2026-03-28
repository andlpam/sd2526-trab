package sd2526.trab.api;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import jakarta.persistence.Id;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import sd2526.trab.client.rest.RestUsersClient;
import sd2526.trab.server.persistence.Hibernate;
import java.util.logging.Logger;
import jakarta.persistence.Entity;
import java.util.List;

@Entity
public class UserInbox {
  @Id
  private String userId;
  @ManyToMany(fetch = FetchType.EAGER)
  @JoinTable(name = "UserMessages")
  private Set<Message> messages;
  private static Logger Log = Logger.getLogger(RestUsersClient.class.getName());

  public UserInbox() {
    this.messages = new HashSet<>();
  }

  public UserInbox(String userId) {
    this.messages = new HashSet<>();
    this.userId = userId;
  }

  public String getUserId() {
    return this.userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public Set<Message> getMessages() {
    return this.messages;
  }

  public boolean checkIfMessageInInbox(Message msg) {
    return messages.contains(msg);
  }

  public void addMessage(Message msg) {
    if (!messages.contains(msg))
      this.messages.add(msg);
  }

  public void removeMessage(Message msg) {
    if (messages.contains(msg))
      this.messages.remove(msg);
    else
      Log.info("The inbox doesn't contain the message that you want to remove");
  }

  public void clearAllMessages() {
    this.messages.clear();
  }

  public Message checkIfMessageInInbox(String msgId) {
    Message msg = null;
    for (Message message : messages) {
      if (message.getId().equals(msgId)) {
        msg = message;
      }
    }
    return msg;
  }

  public List<String> getMessages(String query) {
    List<String> hits = new ArrayList<>();
    for (Message msg : messages) {
      if ((msg.getSubject() != null && msg.getSubject().toLowerCase().contains(query.toLowerCase())) ||
          (msg.getContents() != null && msg.getContents().toLowerCase().contains(query.toLowerCase()))) {
        hits.add(msg.getId());
      }
    }

    return hits;
  }
}
