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
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;

import com.turtlequeue.sdk.api.proto.TurtleQueueGrpc;

import com.turtlequeue.Client;
import com.turtlequeue.ClientImpl;
import com.turtlequeue.Topic;
import com.turtlequeue.TopicBuilderImpl;
import com.turtlequeue.ProducerParams;
import com.turtlequeue.HashingScheme;

public class ProducerBuilder {

  private static final Logger logger = Logger.getLogger(TurtleQueueGrpc.class.getName());

  ClientImpl c = null;

  // easy
  TopicBuilderImpl topicBuilder = null;

  // advanced
  // TODO internal, if made from Producer
  // get a producer_id (unique per SDK)
  // and optionally a producerName?
  // Producer producer = null;
  // Long producerId = null; // unique per SDK ?
  String producerName = null;

  Map<String, String> properties = null; // optional

  TimeUnit batchingMaxPublishDelayUnit = null;
  Long batchingMaxPublishDelayValue = null;
  Integer maxPendingMessages = null;
  Integer batchingMaxMessages = null;
  Boolean enableBatching = null;
  HashingScheme hashingScheme = null;
  TimeUnit sendTimeoutUnit = null;
  Integer sendTimeoutValue = null;

  public ProducerBuilder(ClientImpl c) {
    this.c = c;
    this.topicBuilder = new TopicBuilderImpl();
  }

  public ProducerBuilder namespace (String namespace) {
    this.topicBuilder.namespace(namespace);
    return this;
  }

  public ProducerBuilder persistent (Boolean persistent) {
    this.topicBuilder.persistent(persistent);
    return this;
  }

  public ProducerBuilder producerName (String producerName) {
    this.producerName = producerName;
    return this;
  }

  public ProducerBuilder topic (String topic) {
    this.topicBuilder.topic(topic);
    return this;
  }

  public ProducerBuilder topic (Topic topic) {
    this.topicBuilder.topic(topic);
    return this;
  }

  public ProducerBuilder properties (Map<String, String> properties) {
    this.properties = properties;
    return this;
  }

  public ProducerBuilder property (String key, String value) {
    if(this.properties == null) {
      this.properties = new HashMap<String,String>();
    }
    this.properties.put(key, value);

    return this;
  }

  public ProducerBuilder maxPendingMessages(int maxPendingMessages) {
    this.maxPendingMessages = maxPendingMessages;
    return this;
  }

  public ProducerBuilder enableBatching(boolean enableBatching) {
    this.enableBatching = enableBatching;
    return this;
  }

  public ProducerBuilder hashingScheme(HashingScheme hashingScheme) {
    this.hashingScheme = hashingScheme;
    return this;
  }


    public ProducerBuilder sendTimeout(int sendTimeout, TimeUnit unit) {
    this.sendTimeoutUnit = unit;
    this.sendTimeoutValue = sendTimeout;
    return this;
  }


  // commented until I am convinced there is a good use case
  // public ProducerBuilder setSequenceId (Long sequenceId) {
  //   this.sequenceId = sequenceId;
  //   return this;
  // }

  public CompletableFuture<ProducerImpl> create() {
    ProducerParams conf =  new ProducerParams(this.c.getNextProducerId(), this.topicBuilder.build(), this.producerName, this.enableBatching, this.batchingMaxMessages, this.batchingMaxPublishDelayUnit, this.batchingMaxPublishDelayValue, this.maxPendingMessages, this.properties, this.hashingScheme, this.sendTimeoutUnit, this.sendTimeoutValue);

    return new ProducerImpl(this.c, conf).createReturn();
  }
}