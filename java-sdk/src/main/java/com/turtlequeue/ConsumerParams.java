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

import com.google.common.base.MoreObjects;

//import com.turtlequeue.sdk.api.proto.Tq.SubType;

import com.turtlequeue.SubType;
import com.turtlequeue.Client;
import com.turtlequeue.ClientImpl;
import com.turtlequeue.MessageId;
import com.turtlequeue.Topic;
import com.turtlequeue.EndOfTopicMessageListener;
import com.turtlequeue.SubscriptionMode;
import com.turtlequeue.DeadLetterPolicy;

public class ConsumerParams {

  Long consumerId = null;
  Topic topic = null;
  String subName = null;
  String consumerName = null; // optional
  SubType subType = null;
  Integer priority = null;
  MessageId messageId = null;
  Map<String, String> metadata = null;
  Integer receiverQueueSize = null;
  Long ackTimeout = null;
  TimeUnit ackTimeoutTimeUnit = null;
  EndOfTopicMessageListener endOfTopicMessageListener = null;
  SubscriptionMode subscriptionMode = null;
  String jsonPath = null;
  Boolean enableRetry;
  Long negativeAckRedeliveryDelayValue = null;
  TimeUnit negativeAckRedeliveryDelayUnit = null;
  DeadLetterPolicy deadLetterQueuePolicy = null;

  public ConsumerParams(Long consumerId, Topic topic, String subName, String consumerName, SubType subType, Integer priority, MessageId messageId, Map<String, String> metadata, Integer receiverQueueSize, Long ackTimeout, TimeUnit ackTimeoutTimeUnit, EndOfTopicMessageListener endOfTopicMessageListener, SubscriptionMode subscriptionMode, String jsonPath, Long negativeAckRedeliveryDelayValue, TimeUnit negativeAckRedeliveryDelayUnit, Boolean enableRetry, DeadLetterPolicy deadLetterQueuePolicy) {
    this.consumerId = consumerId;
    this.topic = topic;
    this.subName = subName;
    this.consumerName = consumerName;
    this.subType = subType;
    this.priority = priority;
    this.messageId = messageId;
    this.receiverQueueSize = receiverQueueSize;
    this.metadata = metadata;
    this.ackTimeout = ackTimeout;
    this.ackTimeoutTimeUnit = ackTimeoutTimeUnit;
    this.endOfTopicMessageListener = endOfTopicMessageListener;
    this.subscriptionMode = subscriptionMode;
    this.jsonPath = jsonPath;
    this.negativeAckRedeliveryDelayValue = negativeAckRedeliveryDelayValue;
    this.negativeAckRedeliveryDelayUnit = negativeAckRedeliveryDelayUnit;
    this.enableRetry = enableRetry;
    this.deadLetterQueuePolicy = deadLetterQueuePolicy;
  }

  // getters below

  public Long getConsumerId() {
    return this.consumerId;
  }

  public Boolean getPersistent() {
    return this.topic.getPersistent();
  }

  public String getNamespace() {
    return this.topic.getNamespace();
  }

  public Topic getTopic() {
    return this.topic;
  }

  public String getTopicStr() {
    return this.topic.getTopic();
  }

  public String getSubName() {
    return this.subName;
  }

  public String getConsumerName() {
    return this.consumerName;
  }

  public SubType getSubType() {
    return this.subType;
  }

  public Integer getPriority() {
    return this.priority;
  }

  public MessageId getInitialPosition() {
    return this.messageId;
  }

  public Integer getReceiverQueueSize() {
    return this.receiverQueueSize;
  }

  public Map<String, String> getMetadata() {
    return this.metadata;
  }

  public Long getAckTimeout() {
    return this.ackTimeout;
  }

  public TimeUnit getAckTimeoutTimeUnit() {
    return this.ackTimeoutTimeUnit;
  }

  public EndOfTopicMessageListener getEndOfTopicMessageListener() {
    return this.endOfTopicMessageListener;
  }

  public SubscriptionMode getSubscriptionMode() {
    return this.subscriptionMode;
  }

  public String getJsonPath() {
    return this.jsonPath;
  }

  public Long getNegativeAckRedeliveryDelayValue() {
    return this.negativeAckRedeliveryDelayValue;
  }

  public TimeUnit getNegativeAckRedeliveryDelayUnit() {
    return this.negativeAckRedeliveryDelayUnit;
  }

  public Boolean getEnableRetry() {
    return this.enableRetry;
  }

  public DeadLetterPolicy getDeadLetterQueuePolicy() {
    return this.deadLetterQueuePolicy;
  }

  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("consumerId", consumerId)
      .add("topic", topic)
      .add("subName", subName)
      .add("consumerName", consumerName)
      .add("subType", subType)
      .add("priority", priority)
      .add("messageId", messageId)
      .add("metadata", metadata)
      .add("receiverQueueSize", receiverQueueSize)
      .add("ackTimeout", ackTimeout)
      .add("ackTimeoutTimeUnit", ackTimeoutTimeUnit)
      .add("jsonPath", jsonPath)
      //.add("endOfTopicMessageListener", endOfTopicMessageListener)
      .toString();
  }
}
