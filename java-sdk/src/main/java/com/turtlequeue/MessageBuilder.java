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
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import com.turtlequeue.Client;
import com.turtlequeue.ClientImpl;
import com.turtlequeue.ProducerImpl;
import com.turtlequeue.Message;
import com.turtlequeue.Producer;
import com.turtlequeue.MessageIdImpl;

// used from the producer only
// https://pulsar.apache.org/api/client/org/apache/pulsar/client/api/TypedMessageBuilder.html
//
public class MessageBuilder<T> {
  ClientImpl c = null;
  MessageId messageId = null;
  T data = null;
  String producerName = null;
  Long eventTime = null;
  Long publishTime = null;
  Topic topic = null;
  String key = null;
  Map<String,String> properties = null;
  ProducerImpl<T> producer = null;
  Boolean disableReplication = null;
  Long delay = null;
  TimeUnit delayUnit = null;

  public MessageBuilder(ProducerImpl<T> producer) {
    this.producer = producer;
    this.c = this.producer.getClient();
  }

  public MessageBuilder<T> value(T value) {
    this.data = value;
    return this;
  }

  public MessageBuilder<T> disableReplication() {
    this.disableReplication = true;
    return this;
  }

  public MessageBuilder<T> eventTime(long timestamp) {
    this.eventTime = timestamp;
    return this;
  }

  public MessageBuilder<T> key(String key) {
    this.key = key;
    return this;
  }

  public MessageBuilder<T> properties(Map<String,String> properties) {
    this.properties = properties;
    return this;
  }

  public MessageBuilder<T> property (String key, String value) {
    if(this.properties == null) {
      this.properties = new HashMap<String,String>();
    }
    this.properties.put(key, value);

    return this;
  }

  public MessageBuilder<T> deliverAfter(long delay, TimeUnit unit) {
    this.delay = delay;
    this.delayUnit = unit;
    return this;
  }

  private MessageImpl<T> build() {
    ProducerParams conf = this.producer.getConf();
    return new MessageImpl<T>(this.c,
                              null,
                              null,
                              this.data,
                              conf.getProducerName(),
                              this.eventTime,
                              null,
                              conf.getTopic(),
                              this.key,
                              this.properties,
                              this.producer,
                              this.disableReplication,
                              null,
                              this.delay,
                              this.delayUnit);
  }

  public CompletableFuture<MessageId> send() {
    MessageImpl<T> msg = this.<T>build();
    return this.producer.<T>send(msg);
  }

  // TODO send + format
  // ex. binary

}
