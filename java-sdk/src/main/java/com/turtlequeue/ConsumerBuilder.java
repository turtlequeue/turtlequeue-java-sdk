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

import com.turtlequeue.Client;
import com.turtlequeue.ClientImpl;
import com.turtlequeue.ConsumerParams;
import com.turtlequeue.SubType;
import com.turtlequeue.ConsumerImpl;
import com.turtlequeue.Consumer;
import com.turtlequeue.Topic;
import com.turtlequeue.TopicBuilderImpl;
import com.turtlequeue.SubscriptionMode;
import com.turtlequeue.EndOfTopicMessageListener;

public class ConsumerBuilder {

  ClientImpl c = null;
  TopicBuilderImpl topicBuilder = null;

  EndOfTopicMessageListener endOfTopicMessageListener = null;

  String subName = null; // mandatory
  String consumerName = null; // optional

  SubType subType = null;
  Integer priority = null;
  MessageId initialPosition = null;
  Long ackTimeout = null;
  TimeUnit ackTimeoutTimeUnit = null;
  Map<String, String> metadata = null ;
  Integer receiverQueueSize = null;
  SubscriptionMode subscriptionMode = null;
  String jsonPath = null;

  public ConsumerBuilder(ClientImpl c) {
    this.c = c;
    this.initialPosition = MessageId.latest;
    this.subType = SubType.Exclusive;
    this.priority = 1;
    this.metadata = Collections.<String, String>emptyMap();
    this.receiverQueueSize = 1000;
    this.topicBuilder = new TopicBuilderImpl();
    this.subscriptionMode = SubscriptionMode.Durable;
  }

  public ConsumerBuilder persistent (Boolean persistent) {
    this.topicBuilder.persistent(persistent);
    return this;
  }

  public ConsumerBuilder namespace (String namespace) {
    this.topicBuilder.namespace(namespace);
    return this;
  }

  public ConsumerBuilder topic (String topic) {
    this.topicBuilder.topic(topic);
    return this;
  }

  public ConsumerBuilder topic (Topic topic) {
    this.topicBuilder.topic(topic);
    return this;
  }

  public ConsumerBuilder subscriptionName (String subName) {
    this.subName = subName;
    return this;
  }

  public ConsumerBuilder consumerName (String consumerName) {
    this.consumerName = consumerName;
    return this;
  }

  public ConsumerBuilder priorityLevel (Integer priority) {
    this.priority = priority;
    return this;
  }

  public ConsumerBuilder initialPosition (MessageId messageId) {
    this.initialPosition = messageId;
    return this;
  }

  public ConsumerBuilder metadata (Map<String, String> metadata) {
    this.metadata = metadata;
    return this;
  }

  public ConsumerBuilder receiverQueueSize (Integer receiverQueueSize) {
    this.receiverQueueSize = receiverQueueSize;
    return this;
  }

  public ConsumerBuilder ackTimeout(long ackTimeout, TimeUnit timeUnit) {
    this.ackTimeout = ackTimeout;
    this.ackTimeoutTimeUnit = timeUnit;
    return this;
  }

  public ConsumerBuilder subscriptionMode(SubscriptionMode subscriptionMode) {
    this.subscriptionMode = subscriptionMode;
    return this;
  }

  public ConsumerBuilder endOfTopicMessageListener(EndOfTopicMessageListener endOfTopicMessageListener) {
    this.endOfTopicMessageListener = endOfTopicMessageListener;
    return this;
  }

  public ConsumerBuilder subscriptionType(SubType subType) {
    this.subType = subType;
    return this;
  }


  protected ConsumerBuilder jsonPath(String jsonPath) {
    this.jsonPath = jsonPath;
    return this;
  }

  private ConsumerParams getConsumerParams() {

    if(this.subName == null) {
      throw new IllegalArgumentException("subName must be specified on the ConsumerBuilder object.");
    }

    return new ConsumerParams(this.c.getNextConsumerId(), this.topicBuilder.build(), this.subName, this.consumerName, this.subType, this.priority, this.initialPosition, this.metadata, this.receiverQueueSize, this.ackTimeout, this.ackTimeoutTimeUnit, this.endOfTopicMessageListener, this.subscriptionMode, this.jsonPath);
  }

  public CompletableFuture<Consumer> subscribe() {

    return (CompletableFuture<Consumer>) (CompletableFuture<?>) new ConsumerImpl(this.c, this.getConsumerParams()).subscribeReturn();
  }

  public ConsumerParams create() {
    return this.getConsumerParams();
  }
}