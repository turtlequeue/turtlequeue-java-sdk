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
// implementation detail: grpc client
package com.turtlequeue;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;

import io.grpc.ManagedChannel;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ClientInterceptors;
import io.grpc.ClientInterceptor;
import io.grpc.Context.CancellableContext;

import com.turtlequeue.sdk.api.proto.Tq.ReplyConnect;
import com.turtlequeue.sdk.api.proto.Tq.BrokerToClient;
import com.turtlequeue.sdk.api.proto.TurtleQueueGrpc;
import com.turtlequeue.sdk.api.proto.TurtleQueueGrpc.TurtleQueueStub;
import com.turtlequeue.sdk.api.proto.TurtleQueueGrpc.TurtleQueueBlockingStub;
import com.turtlequeue.GrpcLoggingClientInterceptor;
import com.turtlequeue.AuthenticationCallCredentials;

import com.turtlequeue.sdk.api.proto.TurtleQueueGrpc;


// https://stackoverflow.com/questions/43414695/closing-all-open-streams-in-grpc-java-from-client-end-cleanly
// https://www.mail-archive.com/grpc-io@googlegroups.com/msg00866.html

/**
 * Internal
 **/
public class TqClient {

  private static final Logger logger = Logger.getLogger(TurtleQueueGrpc.class.getName());

  ManagedChannel channel = null;
  TurtleQueueStub asyncStub = null;
  CancellableContext withCancellation = null;
  ClientImpl c = null;

  /**
   * Construct client for accessing Tq reusing the existing channel.
   **/
  public TqClient(ClientImpl c) {
    this.c = c;

    // TODO look at https://grpc.github.io/grpc-java/javadoc/io/grpc/ManagedChannelBuilder.html
    // TODO For the current release, this method may have a side effect that disables Census stats and tracing.
    // GRPC has built-in census stats and tracing???
    // https://opencensus.io/guides/grpc
    //
    //
    ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(c.getHost(), c.getPort());
    if(c.getSecure()) {
      channelBuilder.useTransportSecurity();
    }
    else {
      channelBuilder.usePlaintext();
    }

    // retry/reconnect
    // https://github.com/grpc/grpc-java/issues/5724
    // https://github.com/grpc/grpc-java/blob/master/examples/src/main/resources/io/grpc/examples/retrying/retrying_service_config.json
    // https://github.com/grpc/grpc-java/blob/4be68f32873655fd4fc243eaa636549f4278fb62/examples/src/main/java/io/grpc/examples/retrying/RetryingHelloWorldClient.java#L51-L60
    //
    // https://github.com/googleapis/googleapis/blob/53eb2512a55caabcbad1898225080a2a3dfcb6aa/google/pubsub/v1/pubsub.proto#L487-L495

    Map<String, Object> retryPolicy = new HashMap<>();
    retryPolicy.put("maxAttempts", 999D);
    retryPolicy.put("initialBackoff", "10s");
    retryPolicy.put("maxBackoff", "5s");
    retryPolicy.put("backoffMultiplier", 1D);
    retryPolicy.put("retryableStatusCodes", Arrays.<Object>asList("UNAVAILABLE"));

    Map<String, Object> methodConfigTq = new HashMap<>();
    Map<String, Object> name = new HashMap<>();
    name.put("service", "tq.TurtleQueue"); // FIXME?
    methodConfigTq.put("name", Collections.<Object>singletonList(name));
    methodConfigTq.put("retryPolicy", retryPolicy);
    methodConfigTq.put("waitForReady", false);

    Map<String, Object> methodConfigTqAdm = new HashMap<>();
    Map<String, Object> nameAdm = new HashMap<>();
    nameAdm.put("service", "tqadmin.TurtleQueueAdmin");
    methodConfigTqAdm.put("name", Collections.<Object>singletonList(nameAdm));
    methodConfigTqAdm.put("retryPolicy", retryPolicy);
    methodConfigTqAdm.put("waitForReady", false);

    Map<String, Object> serviceConfig = new HashMap<>();
    serviceConfig.put("methodConfig",
                      Arrays.asList(methodConfigTq, methodConfigTqAdm)
                      //;;Collections.<Object>singletonList(methodConfigTq)
                      );
    // TODO keepAliveTime
    // keepAliveWithoutCalls
    // idleTimeout
    // etc.
    channelBuilder.disableServiceConfigLookUp()
      .enableRetry()
      .defaultServiceConfig(serviceConfig)
      .maxRetryAttempts(999);

    // channel = connection -> will retry
    // do I need to re-create the stubs??
    // .intercept(authInterceptor);

    this.channel = channelBuilder.build();

    TqClient thisRef = this;
    this.withCancellation = Context.current().withCancellation();

    withCancellation.run(new Runnable() {
        @Override public void run() {
          thisRef.asyncStub = TurtleQueueGrpc.newStub(ClientInterceptors.intercept(channel, new ClientInterceptor[] { new GrpcLoggingClientInterceptor()}))
            .withCallCredentials(new AuthenticationCallCredentials(thisRef.c.getUserToken(),
                                                                   thisRef.c.getApiKey(),
                                                                   thisRef.c.getAuthMethod(),
                                                                   thisRef.c.getDataFormat(),
                                                                   thisRef.c.getSdkVersion()));
        }});
  }

  protected TurtleQueueStub checkAndGetStub(Boolean requestConnection) throws Exception {
    if (channel == null) {
      throw new Exception("Internal error: null channel when initializing TqClient");
    } else {

      ConnectivityState state = channel.getState(requestConnection) ;

      logger.log(Level.FINE, "Initializing TqClient requestConnection={0}, ConnectivityState={1} ", new Object[]{requestConnection,state });

      if (requestConnection == false && state != ConnectivityState.READY) {
        throw new Exception("Trying to use the methods before the connection has been initialized, state = " + state);
      }
      return this.asyncStub;
    }
  }

  protected ConnectivityState getState(){
    return this.channel.getState(false);
  }

  protected ConnectivityState reconnectChannel() {
    logger.log(Level.FINE , "Reconnecting channel " + c.getHost() + ":" + c.getPort());

    ConnectivityState st = this.channel.getState(true);
    this.channel.resetConnectBackoff();
    // return a new stub instead??

    return st;
  }

  protected synchronized void close() throws Exception {
    //
    // https://grpc.github.io/grpc-java/javadoc/io/grpc/ManagedChannel.html#notifyWhenStateChanged-io.grpc.ConnectivityState-java.lang.Runnable-//
    // https://github.com/grpc/grpc-java/issues/6642
    // https://stackoverflow.com/questions/63250570/how-to-cancel-a-grpc-streaming-call
    // https://github.com/grpc/grpc-java/issues/3268#issuecomment-317484178
    // https://github.com/grpc/grpc-java/issues/3297#issuecomment-346427628
    //

    if (this.channel.isShutdown()) {
      logger.log(Level.WARNING , "Trying to close a client already closed");
      return ;
    }

    try {
      logger.log(Level.FINE , "Closing Client Down");

      this.withCancellation.cancel(new Exception("Client is closing"));

      this.channel.shutdown();
      if (!this.channel.awaitTermination(2500, TimeUnit.MILLISECONDS)) {
         logger.log(Level.INFO, "Timed out trying to gracefully shut down the connection: {0}. ", this.channel);
      }
    } catch (Exception ex) {
      logger.log(Level.INFO, "Unexpected exception while waiting for channel termination", ex);
    }

    if(!this.channel.isShutdown() || !this.channel.isTerminated()) {
      try {
        logger.log(Level.FINE, "Shutting down Client now");
        this.channel.shutdownNow();
        if (!this.channel.awaitTermination(2500, TimeUnit.MILLISECONDS)) {
          logger.log(Level.FINE, "Timed out forcefully shutting down connection: {0}. ", this.channel);
        }
      } catch (Exception ex) {
        logger.log(Level.WARNING , "Unexpected Exception while waiting for channel termination\n {0}", ex);
      }
    } else {
      //logger.log(Level.INFO , "Client has shut down now");
    }

    if(this.channel.isTerminated()) {
      logger.log(Level.FINE, "Client has shut down");
    } else {
      logger.log(Level.WARNING , "Giving up closing the client");
    }

  }

  // for the admin stub, allows sharing the connection
 // ManagedChannel ?
  protected Channel getChannel () {
    return this.channel;
  }
}
