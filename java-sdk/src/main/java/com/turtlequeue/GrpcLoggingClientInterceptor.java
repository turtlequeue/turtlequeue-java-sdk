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

import java.util.logging.Level;
import java.util.logging.Logger;

//import com.google.common.annotations.VisibleForTesting;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import com.turtlequeue.sdk.api.proto.TurtleQueueGrpc;

// see
// https://github.com/grpc/grpc-java/blob/81da3eb95be37fa0647ce8da2e19de96ab84c600/examples/src/main/java/io/grpc/examples/header/HeaderClientInterceptor.java#L33
// https://github.com/grpc/grpc-java/blob/d35fbd7eee072854c20b61ea7fcf7c70a8e8cb8e/examples/src/test/java/io/grpc/examples/header/HeaderClientInterceptorTest.java#L87
// https://grpc.github.io/grpc-java/javadoc/io/grpc/stub/ClientCallStreamObserver.html
//
// https://groups.google.com/g/grpc-io/c/93YrgPyxeAA?pli=1//

/**
 * A interceptor to handle client to broker messages logging
 */
public class GrpcLoggingClientInterceptor implements ClientInterceptor {

  private static final Logger logger = Logger.getLogger(TurtleQueueGrpc.class.getName());

  private String prefix = null;

  public GrpcLoggingClientInterceptor() {
    this.prefix = "";
  }

  public GrpcLoggingClientInterceptor (String p) {
    this.prefix = p;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                             CallOptions callOptions, Channel next) {

    String s = this.prefix + "Client says: {0} ";
    return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {

      @Override
      public void sendMessage(ReqT message) {
        logger.log(Level.INFO, s, message);
        super.sendMessage(message);
      }

    };
  };
};
