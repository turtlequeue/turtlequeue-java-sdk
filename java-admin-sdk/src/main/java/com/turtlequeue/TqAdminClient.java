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

import io.grpc.ManagedChannel;
import io.grpc.Channel;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ClientInterceptors;
import io.grpc.ClientInterceptor;

import com.turtlequeue.sdk.api.proto.TurtleQueueAdminGrpc;
import com.turtlequeue.sdk.api.proto.TurtleQueueAdminGrpc.TurtleQueueAdminStub;

import com.turtlequeue.GrpcLoggingClientInterceptor;

/**
 * Internal
 **/
public class TqAdminClient {

  ManagedChannel channel = null;
  TurtleQueueAdminStub asyncStub = null;
  //TurtleQueueBlockingStub blockingStub = null;

  /**
   * Construct client for accessing Tq reusing the existing channel.
   **/
  public TqAdminClient(String host, int port, Boolean secure) {
    ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(host, port);
    if(secure) {
      channelBuilder.useTransportSecurity();
    }
    else {
      channelBuilder.usePlaintext();
    }

    this.channel = channelBuilder.build();
    // this.asyncStub = TurtleAdminQueueGrpc.newStub(channel);
    this.asyncStub = TurtleQueueAdminGrpc.newStub(ClientInterceptors.intercept(channel, new ArrayList<ClientInterceptor>() {{
      add(new GrpcLoggingClientInterceptor());
    }}));

  }

  public TurtleQueueAdminStub checkAndGetStub(Boolean requestConnection) throws Exception {
    if (channel == null) {
      throw new Exception("Internal error: null channel when initializing TqClient");
    } else {
      ConnectivityState state = channel.getState(requestConnection) ;
      if (requestConnection == false && state != ConnectivityState.READY) {
        throw new Exception("Trying to use the methods before the connection has been initialized, state = " + state);
      }
      return this.asyncStub;
    }
  }

  public ConnectivityState getState(){
    return this.channel.getState(false);
  }

  public ManagedChannel close() throws Exception {
    return this.channel.shutdown();
  }
}
