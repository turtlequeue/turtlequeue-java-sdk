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

import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import com.turtlequeue.sdk.api.proto.Tq.CommandConsumer;
import com.turtlequeue.sdk.api.proto.Tq.CommandAck;

import com.turtlequeue.ClientImpl;
import com.turtlequeue.ConsumerImpl;
import com.turtlequeue.MessageId;


public class AcknowledgeBuilder {

  public enum AckType {
    Individual, Cumulative
  }

  ClientImpl c = null;
  ConsumerImpl consumer = null;
  MessageId messageId = null;
  AckType ackType = null;
  boolean negativeAcknowledge;

  public AcknowledgeBuilder(ClientImpl c) {
    this.c = c;
    this.ackType = AckType.Individual;
  }

  public AcknowledgeBuilder setMessage (Message message) {
    this.messageId = message.getMessageId();
    return this;
  }


  public AcknowledgeBuilder setConsumer (Consumer consumer) {
    this.consumer = (ConsumerImpl) consumer;
    return this;
  }

  public AcknowledgeBuilder setMessageId (MessageId messageId) {
    this.messageId = messageId;
    return this;
  }

  public AcknowledgeBuilder setAckType (AckType ackType) {
    this.ackType = ackType;
    return this;
  }

  public AcknowledgeBuilder setNegativeAck(boolean b) {
    this.negativeAcknowledge = b;
    return this;
  }

  public CompletableFuture<Void> nack () {
    this.negativeAcknowledge = true;
    return this.send();
  }

  public CompletableFuture<Void> ack () {
    this.negativeAcknowledge = false;
    return this.send();
  }

  public CompletableFuture<Void> send() {

    if(this.messageId == null) {
      throw new IllegalArgumentException(".setMessage or .setMessageId must be called before acknowledging");
    }

    if(this.consumer == null) {
      throw new IllegalArgumentException(".setConsumer must be called before acknowledging");
    }

    CommandAck.AckType ack ;

    switch (ackType) {
    case Individual:
      ack = CommandAck.AckType.INDIVIDUAL;
      break;

    case Cumulative:
      ack = CommandAck.AckType.CUMULATIVE;
      break;

    default:
      ack =  CommandAck.AckType.INDIVIDUAL;
      break;
    }

    return this.c.consumerCommand(CommandConsumer.newBuilder()
                                  .setConsumerId(this.consumer.getConsumerId())
                                  .setCommandAck(CommandAck.newBuilder()
                                                 .setAckType(ack)
                                                 .setMessageId(MessageIdImpl.toMessageIdData(this.messageId))
                                                 .setNegativeAck(this.negativeAcknowledge)
                                                 .build())
                                  .build());

  }
}