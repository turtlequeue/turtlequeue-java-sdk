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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.MoreObjects;

import com.turtlequeue.Message;
import com.turtlequeue.Producer;
import com.turtlequeue.ClientImpl;
import com.turtlequeue.MessageIdImpl;

public class MessageImpl<T> implements Message<T> {
  ClientImpl c = null;
  ConsumerImpl<T> consumer = null;
  MessageId messageId = null;
  // Object? or parametrized <T> ?
  T data = null;
  String producerName = null;
  Long eventTime = null;
  Long publishTime = null;
  Topic topic = null;
  String key = null;
  Map<String,String> properties = null;
  ProducerImpl<T> producer = null;
  Boolean isReplicated = null;
  String replicatedFrom = null;
  Long delay = null;
  TimeUnit delayTimeUnit = null;

  public MessageImpl (ClientImpl c, ConsumerImpl<T> consumer, MessageIdImpl messageId, T payload, String producerName, Long eventTime, Long publishTime, Topic topic, String key, Map<String, String> properties, ProducerImpl<T> producer, Boolean isReplicated, String replicatedFrom, Long delay, TimeUnit delayTimeUnit) {
    this.c = c;
    this.consumer = consumer;
    this.messageId = messageId;
    this.data = payload;
    this.producerName = producerName;
    this.eventTime = eventTime;
    this.publishTime = publishTime;
    this.topic = topic;
    this.key = key;
    this.properties = properties;
    this.producer = producer;
    this.isReplicated = isReplicated;
    this.replicatedFrom = replicatedFrom;
    this.delay = delay;
    this.delayTimeUnit = delayTimeUnit;
  }

  public MessageId getMessageId() {
    return this.messageId;
  }

  public T getData() {
    return this.data;
  }

  public Consumer<T> getConsumer() {
    return this.consumer;
  }

  public Client getClient() {
    return this.c;
  }

  public String getProducerName() {
    return this.producerName;
  }

  public Long getEventTime() {
    return this.eventTime;
  }

  public Long getPublishTime() {
    return this.publishTime;
  }

  public boolean isReplicated() {
    if(isReplicated != null) {
      return this.isReplicated;
    } else {
      return true;
    }
  }

  public String getReplicatedFrom() {
    return this.replicatedFrom;
  }

  public Topic getTopic() {
    if(this.topic != null && this.topic.getTopic() != null) {
      return this.topic;
    } else if (this.consumer != null) {
      return this.consumer.getTopic();
    }
    return null;
  }

  public Map<String,String> getProperties() {
    return this.properties;
  }

  public String getKey() {
    return this.key;
  }

  public CompletableFuture<Void> acknowledge() {
    return this.consumer.acknowledge(this);
  }

  public Long getDelayValue() {
    return this.delay;
  }

  public TimeUnit getDelayTimeUnit() {
    return this.delayTimeUnit;
  }

  public Producer<T> getProducer() {
    return this.producer;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof Message)) {
      return false;
    }

    Message t = (Message) o;

    return Objects.equals(this.getTopic(), t.getTopic()) && Objects.equals(this.getMessageId(), t.getMessageId()) && Objects.equals(this.getProducerName(), t.getProducerName()) && Objects.equals(this.getEventTime(), t.getEventTime()) && Objects.equals(this.getPublishTime(), t.getPublishTime()) && Objects.equals(this.getTopic(), t.getTopic()) && Objects.equals(this.getKey(), t.getKey()) && Objects.equals(this.getProperties(), t.getProperties()) && Objects.equals(this.getProducer(), t.getProducer());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getTopic(), this.getProducerName(), this.getMessageId(), this.getEventTime(), this.getKey(), this.getPublishTime(), this.getProducer());
  }

  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("topic", topic)
      .add("producerName", producerName)
      //.add("data", data) // TODO hide? truncate? Type? Size?
      .add("eventTime", eventTime)
      .add("publishTime", publishTime)
      .add("key", key)
      .toString();
  }
}