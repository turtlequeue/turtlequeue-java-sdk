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

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Map;

import com.google.protobuf.ByteString;

import com.cognitect.transit.TransitFactory;
import com.cognitect.transit.Reader;
import com.cognitect.transit.Writer;

import com.turtlequeue.sdk.api.proto.TurtleQueueGrpc;
import com.turtlequeue.sdk.api.proto.Tq.CommandProducer;
import com.turtlequeue.sdk.api.proto.Tq.CommandCloseProducer;
import com.turtlequeue.sdk.api.proto.Tq.CommandSend;
import com.turtlequeue.sdk.api.proto.Tq.ResponsePublish;

import com.turtlequeue.Client;
import com.turtlequeue.ClientImpl;
import com.turtlequeue.Producer;
import com.turtlequeue.MessageId;
import com.turtlequeue.ProducerPossibleStates;
import com.turtlequeue.MessageBuilder;
import com.turtlequeue.StateMachine;
import com.turtlequeue.Encoder;

public class ProducerImpl<T> implements Producer {

  private static final Logger logger = Logger.getLogger(TurtleQueueGrpc.class.getName());

  ClientImpl c = null;
  ProducerParams conf = null;
  StateMachine<ProducerPossibleStates> stateMachine = null;
  CompletableFuture<ProducerImpl<T>> producerCreateReturnF = null;

  ProducerImpl(ClientImpl client, ProducerParams conf) {

    this.c = client;
    this.conf = conf;

    this.stateMachine = new StateMachine<ProducerPossibleStates>().setState(ProducerPossibleStates.Idle); // Idle

    this.c.registerProducer(this);

    this.producerCreateReturnF = new CompletableFuture<ProducerImpl<T>>();

    this.c.registerProducerBroker(this).thenRun(() -> {
        logger.log(Level.FINE, "[{0}] Registering producer success", conf);

        this.stateMachine.setState(ProducerPossibleStates.Ready);
        producerCreateReturnF.complete(this);
      }).exceptionally((ex) -> {
          logger.log(Level.WARNING, "[{0}]Registering producer broker failed {1}", new Object[]{conf, ex});
          this.stateMachine.setState(ProducerPossibleStates.Stopping);
          producerCreateReturnF.completeExceptionally(ex);
          return null;
        });

  }

  protected CompletableFuture<ProducerImpl<T>> createReturn() {
    return this.producerCreateReturnF;
  }

  public MessageBuilder<T> newMessage() {
    return new MessageBuilder<T>(this);
  }

  protected CompletableFuture<MessageId> send(Message<T> msg) {

    // Write the data to a stream
    // https://github.com/cognitect/transit-java/blob/8fdb4d68c4ee0a9b21b38ef6009f28633d87e734/src/main/java/com/cognitect/transit/TransitFactory.java#L53
    // Problem: need to accept the Clojure types
    ByteString.Output out = com.google.protobuf.ByteString.newOutput(); // = new ByteArrayOutputStream();

    Writer<T> writer = TransitFactory.writer(TransitFactory.Format.JSON, out, this.c.getCustomWriteHandlers(), this.c.getCustomDefaultWriteHandler());

    try {
      writer.write(msg.getData());
    } catch (Exception ex) {
      logger.log(Level.WARNING, "[{0}] Failed to encode data", msg.getData());
      throw (ex);
    }

    CommandSend.Builder b = CommandSend.newBuilder();

    b.setPayload(out.toByteString())
      .setReplicationDisabled(!msg.isReplicated());


    if(msg.getKey() != null) {
      b.setKey(msg.getKey());
    }

    if(msg.getEventTime() != null) {
      b.setEventTime(msg.getEventTime());
    }

    if (msg.getDelayValue() != null && msg.getDelayTimeUnit() != null) {
      b.setDelayTimeValue(msg.getDelayValue())
        .setDelayTimeUnit(Encoder.javaTimeUnitToProtobuf(msg.getDelayTimeUnit()));
    }

    if(msg.getProperties() != null) {
      Map<String,String> p = msg.getProperties();
      b.putAllProperties(p);
    }

    return this.c.<ResponsePublish>producerCommand(CommandProducer.newBuilder()
                                                                               .setProducerId(this.getProducerId())
                                                                               .setCommandSend(b.build())
                                                                               .build())
      .thenApply((ResponsePublish rp) -> {
          MessageId messageId = MessageId.fromMessageIdData(rp.getMessageId());

          return messageId;
        });
  }

  protected Long getProducerId() {
    return this.conf.getProducerId();
  }

  protected ProducerParams getConf() {
    return this.conf;
  }

  protected ClientImpl getClient() {
    return this.c;
  }

  protected void reconnect() {

    this.c.registerProducerBroker(this).thenRun(() -> {
        logger.log(Level.FINE, "[{0}] Re-registering producer success", conf);
        this.stateMachine.setState(ProducerPossibleStates.Ready);
      }).exceptionally((ex) -> {
          logger.log(Level.WARNING, "[{0}]Re-registering producer with the broker failed {1}", new Object[]{conf, ex});
          this.stateMachine.setState(ProducerPossibleStates.Stopping);
          return null;
        });
  }


  protected CompletableFuture<Void> closeAsync(boolean informBroker) {

    this.stateMachine.setState(ProducerPossibleStates.Stopping);

    if(informBroker == true) {
      ClientImpl clientRef = this.c;

      return this.c.producerCommand(CommandProducer.newBuilder()
                                    .setProducerId(this.getProducerId())
                                    .setCommandCloseProducer(CommandCloseProducer.newBuilder()
                                                             .build())
                                    .build())
        .thenRun(() -> {
            this.stateMachine.setState(ProducerPossibleStates.Idle);
            clientRef.removeProducer(this);
          });

    } else {
      this.stateMachine.setState(ProducerPossibleStates.Idle);
      this.c.removeProducer(this);
      return CompletableFuture.supplyAsync(() -> {
          return null;
        });
    }
  }

  @Override
  public void close() {
    this.closeAsync(true).join();
  }


}
