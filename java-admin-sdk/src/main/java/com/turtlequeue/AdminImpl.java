/*
 * Copyright © 2019 Turtlequeue limited (hello@turtlequeue.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.turtlequeue;

import java.util.ArrayList;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.grpc.ClientInterceptors;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.FutureCallback;

import com.turtlequeue.sdk.api.proto.Tqadmin;
import com.turtlequeue.sdk.api.proto.TurtleQueueGrpc;
import com.turtlequeue.sdk.api.proto.TurtleQueueAdminGrpc;
import com.turtlequeue.sdk.api.proto.TurtleQueueAdminGrpc.TurtleQueueAdminStub;
import com.turtlequeue.sdk.api.proto.TurtleQueueAdminGrpc.TurtleQueueAdminFutureStub;

import com.turtlequeue.sdk.api.proto.Tqadmin.TopicDeleteRequest;
import com.turtlequeue.sdk.api.proto.Tqadmin.TopicDeleteResponse;
import com.turtlequeue.sdk.api.proto.Tqadmin.TopicTerminateRequest;
import com.turtlequeue.sdk.api.proto.Tqadmin.TopicTerminateResponse;

import com.turtlequeue.Admin;
import com.turtlequeue.Topic;
import com.turtlequeue.TopicImpl;
import com.turtlequeue.GrpcLoggingClientInterceptor;
import com.turtlequeue.ClientImpl;
import com.turtlequeue.AuthenticationCallCredentials;
import com.turtlequeue.MessageId;

public class AdminImpl implements Admin {

  private static final Logger logger = Logger.getLogger(TurtleQueueGrpc.class.getName());

  TurtleQueueAdminFutureStub stub = null;
  ClientImpl c = null;
  ExecutorService executor = null;

  public AdminImpl (ClientImpl c) {

    this.c = c;
    this.executor = Executors.newFixedThreadPool(2);

    this.stub = TurtleQueueAdminGrpc.newFutureStub(ClientInterceptors.intercept(this.c.tqClient.getChannel(), new ArrayList<ClientInterceptor>() {{
      add(new GrpcLoggingClientInterceptor("[Admin] "));
    }}))
      // auth with every call in metadata (headers)
      .withCallCredentials(new AuthenticationCallCredentials(this.c.getUserToken(),
                                                             this.c.getApiKey(),
                                                             this.c.getAuthMethod(),
                                                             this.c.getDataFormat(),
                                                             this.c.getSdkVersion()));

  }

  static public void initialize() {
    ClientImpl.registerAdmin(AdminImpl.class);
  }

  protected <T>  CompletableFuture<T> toCompletableFuture(ListenableFuture<T> f) {
    CompletableFuture<T> res = new CompletableFuture<T>();

    Futures.addCallback(f, new FutureCallback<T>() {
        @Override
        public void onSuccess(T val) {
          res.complete(val);
        }

        @Override
        public void onFailure(Throwable t) {
          res.completeExceptionally(t);
        }
      },
      this.executor);

    return res;
  }

  public CompletableFuture<Void> deleteTopic(Topic t, boolean force, boolean deleteSchema) {
    final ListenableFuture<TopicDeleteResponse> response = this.stub.topicDelete(TopicDeleteRequest.newBuilder()
                                                                                 .setTopic(TopicImpl.toTqTopic(t))
                                                                                 .setForce(force)
                                                                                 .setDeleteSchema(deleteSchema)
                                                                                 .build());

    return this.<TopicDeleteResponse>toCompletableFuture(response)
      .thenApply(topicResponse -> {
          // topic response is effectively null
          return null;
        });
  }

  public CompletableFuture<Void> deleteTopic(Topic t, boolean force) {
    return this.deleteTopic(t, force, false);
  }

  public CompletableFuture<Void> deleteTopic(Topic t) {
    return this.deleteTopic(t, false, false);
  }

  public CompletableFuture<MessageId> terminateTopic(Topic t) {
    final ListenableFuture<TopicTerminateResponse> response = this.stub.topicTerminate(TopicTerminateRequest.newBuilder()
                                                                                       .setTopic(TopicImpl.toTqTopic(t))
                                                                                       .build());

    return this.<TopicTerminateResponse>toCompletableFuture(response)
      .thenApply((TopicTerminateResponse termResponse) -> {
          return MessageId.fromMessageIdData(termResponse.getMessageId());
        });
  }



}




//
// Resources:
// https://github.com/grpc/grpc-java/blob/master/examples/src/main/java/io/grpc/examples/routeguide/RouteGuideClient.java
// https://github.com/grpc/grpc-java/blob/0b6f29371bd96614fdbdcd3638d4bb6312258da3/examples/src/main/java/io/grpc/examples/routeguide/RouteGuideClient.java#L264-L272
//
