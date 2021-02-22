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
import java.util.concurrent.CompletableFuture;

import com.google.common.base.MoreObjects;

import com.turtlequeue.sdk.api.proto.TurtleQueueGrpc;

import com.turtlequeue.Reader;
import com.turtlequeue.Topic;
import com.turtlequeue.MessageId;
import com.turtlequeue.ConsumerImpl;
import com.turtlequeue.ConsumerParams;

//
// https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ReaderImpl.java
// https://github.com/apache/pulsar/commit/7badf1ab833f36319edb5e373e76966ed74958ab
// really just a wrapper around a consumer
// no specific proto messages here
// unless we want a RPC shortcut to avoid reimplementing something
// (which should not happen)
//
public class ReaderImpl<T> implements Reader<T> {

  private static final Logger logger = Logger.getLogger(TurtleQueueGrpc.class.getName());

  ConsumerImpl<T> consumer = null;

  ReaderImpl(ClientImpl c, ConsumerParams conf) {
    this.consumer = new ConsumerImpl<T>(c, conf);
  }


  public Topic getTopic() {
    return consumer.getTopic();
  }

  public CompletableFuture<Message<T>> readNext() {
    CompletableFuture<Message<T>> receiveFuture = consumer.receive();
    receiveFuture.whenComplete((msg, t) -> {
        if (msg != null) {
          consumer.acknowledgeCumulativeAsync(msg);
        }
      });
    return receiveFuture;
  }

  public CompletableFuture<Void> closeAsync() {
    return consumer.closeAsync(true);
  }

  public void close() {
    this.closeAsync().join();
  }

  public boolean hasReachedEndOfTopic() {
    return consumer.hasReachedEndOfTopic();
  }

  public CompletableFuture<Boolean> hasMessageAvailable() {
    return consumer.hasMessageAvailable();
  }

  public boolean isConnected() {
    return consumer.isConnected();
  }

  public CompletableFuture<Void> seek(MessageId messageId) {
    return consumer.seek(messageId);
  }

  public CompletableFuture<Void> seek(long timestamp) {
    return consumer.seek(timestamp);
  }

  public CompletableFuture<Reader<T>> subscribeReturn() {
    return consumer.subscribeReturn()
      .thenApply(consumer -> {
          return this;
        });
  }


  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("consumerId", this.consumer.consumerId)
      .add("subscription", this.consumer.subName)
      .add("readerName", this.consumer.consumerName)
      .add("topic", this.consumer.topic)
      //.add("receiverQueueSize", this.consumer.maxReceiverQueueSize)
      .toString();
  }
}