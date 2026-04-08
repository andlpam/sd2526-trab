package sd2526.trab.client.grpc;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sd2526.trab.api.User;
import sd2526.trab.api.grpc.GrpcUsersGrpc;
import sd2526.trab.api.grpc.Users.*;
import sd2526.trab.api.java.Users;
import sd2526.trab.api.java.Result;
import sd2526.trab.server.grpc.utils.DataModelAdaptor;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GrpcUsersClient extends GrpcClient implements Users {

    private final GrpcUsersGrpc.GrpcUsersBlockingStub stub;

    public GrpcUsersClient(URI serverUri) {
        Channel channel = ManagedChannelBuilder.forAddress(serverUri.getHost(), serverUri.getPort())
            .enableRetry()
            .usePlaintext()
            .build();
        this.stub = GrpcUsersGrpc.newBlockingStub(channel);
    }

    @Override
    public Result<String> postUser(User user) {
        return super.reTry(() -> {
            GrpcUser grpcUser = DataModelAdaptor.User_to_GrpcUser(user);
            PostUserResult res = stub.postUser(grpcUser);
            return Result.ok(res.getUserAddress());
        });
    }

    @Override
    public Result<User> getUser(String name, String pwd) {
        return super.reTry(() -> {
            GetUserArgs request = GetUserArgs.newBuilder().setName(name).setPwd(pwd).build();
            GetUserResult res = stub.getUser(request);
            return Result.ok(DataModelAdaptor.GrpcUser_to_User(res.getUser()));
        });
    }

    @Override
    public Result<User> updateUser(String name, String pwd, User info) {
        return super.reTry(() -> {
            GrpcUser new_info = DataModelAdaptor.User_to_GrpcUser(info);
            UpdateUserArgs request = UpdateUserArgs.newBuilder()
                .setName(name).setPwd(pwd).setInfo(new_info).build();
            UpdateUserResult res = stub.updateUser(request);
            return Result.ok(DataModelAdaptor.GrpcUser_to_User(res.getUser()));
        });
    }

    @Override
    public Result<User> deleteUser(String name, String pwd) {
        return super.reTry(() -> {
            DeleteUserArgs request = DeleteUserArgs.newBuilder().setName(name).setPwd(pwd).build();
            DeleteUserResult res = stub.deleteUser(request);
            return Result.ok(DataModelAdaptor.GrpcUser_to_User(res.getUser()));
        });
    }

    @Override
    public Result<List<User>> searchUsers(String name, String pwd, String query) {
        return super.reTry(() -> {
            SearchUsersArgs request = SearchUsersArgs.newBuilder()
                .setName(name).setPwd(pwd).setQuery(query).build();
            Iterator<GrpcUser> res = stub.searchUsers(request);
            List<User> ret = new ArrayList<>();
            while (res.hasNext()) {
                ret.add(DataModelAdaptor.GrpcUser_to_User(res.next()));
            }
            return Result.ok(ret);
        });
    }
}