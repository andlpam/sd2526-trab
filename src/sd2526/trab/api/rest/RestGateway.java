package sd2526.trab.api.rest;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import java.util.List;

@Path("/")
public interface RestGateway {

  @POST
  @Path("/users")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  String postUser(User user);

  @GET
  @Path("/users/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  User getUser(@PathParam("name") String name, @QueryParam("pwd") String pwd);

  @PUT
  @Path("/users/{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  User updateUser(@PathParam("name") String name, @QueryParam("pwd") String pwd, User info);

  @DELETE
  @Path("/users/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  User deleteUser(@PathParam("name") String name, @QueryParam("pwd") String pwd);

  @GET
  @Path("/users")
  @Produces(MediaType.APPLICATION_JSON)
  List<User> searchUsers(@QueryParam("name") String name, @QueryParam("pwd") String pwd,
      @QueryParam("query") String pattern);

  @POST
  @Path("/messages")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  String postMessage(@QueryParam("pwd") String pwd, Message msg);

  @GET
  @Path("/messages/{user}/{mid}")
  @Produces(MediaType.APPLICATION_JSON)
  Message getMessage(@PathParam("user") String user, @PathParam("mid") String mid, @QueryParam("pwd") String pwd);

  @GET
  @Path("/messages/{user}")
  @Produces(MediaType.APPLICATION_JSON)
  List<String> getMessages(@PathParam("user") String user, @QueryParam("pwd") String pwd,
      @QueryParam("query") String query);

  @DELETE
  @Path("/messages/{user}/{mid}")
  void removeFromUserInbox(@PathParam("user") String user, @PathParam("mid") String mid, @QueryParam("pwd") String pwd);

  @DELETE
  @Path("/messages/msg/{user}/{mid}")
  void deleteMessage(@PathParam("user") String user, @PathParam("mid") String mid, @QueryParam("pwd") String pwd);
}