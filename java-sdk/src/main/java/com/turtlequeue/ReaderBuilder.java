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
import java.util.UUID;
import org.apache.commons.codec.digest.DigestUtils;

import com.turtlequeue.Client;
import com.turtlequeue.ClientImpl;
import com.turtlequeue.ConsumerParams;
import com.turtlequeue.SubType;
import com.turtlequeue.ConsumerImpl;
import com.turtlequeue.Consumer;
import com.turtlequeue.Topic;

public class ReaderBuilder {

  ClientImpl c = null;
  ConsumerBuilder confBuilder = null;

  public ReaderBuilder(ClientImpl c) {
    this.c = c;
    this.confBuilder = new ConsumerBuilder(c);
  }

  public ReaderBuilder namespace (String namespace) {
    this.confBuilder.namespace(namespace);
    return this;
  }

  public ReaderBuilder topic (Topic topic) {
    this.confBuilder.topic(topic);
    return this;
  }

  public ReaderBuilder readerName (String readerName) {
    // OR generate a name
    this.confBuilder.subscriptionName(readerName);
    return this;
  }

  public ReaderBuilder consumerName(String readerName) {
    this.confBuilder.consumerName(readerName);
    return this;
  }

  public ReaderBuilder initialPosition (MessageId messageId) {
    this.confBuilder.initialPosition(messageId);
    return this;
  }

  public ReaderBuilder ackTimeout(long ackTimeout, TimeUnit timeUnit) {
    this.confBuilder.ackTimeout(ackTimeout, timeUnit);
    return this;
  }

  public ReaderBuilder jsonPath(String jsonPath) {
    this.confBuilder.jsonPath(jsonPath);
    return this;
  }

  public CompletableFuture<Reader> create() {

    // if(this.confBuilder.topicBuilder == null) {
    //   throw new IllegalArgumentException("topic must be specified on the ReaderBuilder object.");
    // }

    // if(this.confBuilder.namespace == null) {
    //   throw new IllegalArgumentException("namespace must be specified on the ReaderBuilder object.");
    // }

    if(this.confBuilder.subName == null) {
      // set a default, SHA1 etc
      String subscription = "reader-" + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 10);
      this.confBuilder.subscriptionName(subscription);
    }

    this.confBuilder.subscriptionMode(SubscriptionMode.NonDurable);
    return (CompletableFuture<Reader>) (CompletableFuture<?>) new ReaderImpl(this.c, this.confBuilder.create()).subscribeReturn();
  }

}