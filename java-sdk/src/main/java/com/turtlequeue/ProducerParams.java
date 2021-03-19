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
import java.util.concurrent.TimeUnit;

import com.google.common.base.MoreObjects;

import com.turtlequeue.Client;
import com.turtlequeue.Topic;
import com.turtlequeue.ClientImpl;
import com.turtlequeue.HashingScheme;
import com.turtlequeue.CompressionType;

// immutable parameters for a producer
public class ProducerParams {

  Long producerId = null;
  Topic topic = null;
  String producerName = null; // optional
  Boolean enableBatching = null;
  Integer batchingMaxMessages = null;
  TimeUnit batchingMaxPublishDelayUnit = null;
  Long batchingMaxPublishDelayValue = null;
  Integer maxPendingMessages = null;
  TimeUnit sendTimeoutUnit = null;
  Integer sendTimeoutValue = null;
  Map<String, String> properties = null;
  HashingScheme hashingScheme = null;
  CompressionType compressionType = null;
  Boolean blockIfQueueFull = null;

    public ProducerParams(Long producerId, Topic topic, String producerName, Boolean enableBatching, Integer batchingMaxMessages, TimeUnit batchingMaxPublishDelayUnit, Long batchingMaxPublishDelayValue, Integer maxPendingMessages, Map<String, String> properties, HashingScheme hashingScheme, TimeUnit sendTimeoutUnit, Integer sendTimeoutValue, Boolean blockIfQueueFull) {
    this.producerId = producerId;
    this.topic = topic;
    this.producerName = producerName;
    this.enableBatching = enableBatching;
    this.batchingMaxMessages = batchingMaxMessages;
    this.batchingMaxPublishDelayUnit = batchingMaxPublishDelayUnit;
    this.batchingMaxPublishDelayValue = batchingMaxPublishDelayValue;
    this.maxPendingMessages = maxPendingMessages;
    this.sendTimeoutUnit = sendTimeoutUnit;
    this.sendTimeoutValue = sendTimeoutValue;
    this.properties = properties;
    this.hashingScheme = hashingScheme;
    this.blockIfQueueFull = blockIfQueueFull;
  }

  public long getProducerId() {
    return this.producerId;
  }

  public Topic getTopic() {
    return this.topic;
  }

  public Boolean getPersistent() {
    return this.topic.getPersistent();
  }

  public String getNamespace() {
    return this.topic.getNamespace();
  }

  public String getProducerName() {
    return this.producerName;
  }

  public Boolean getEnableBatching() {
    return this.enableBatching;
  }

  public Integer batchingMaxMessages() {
    return this.batchingMaxMessages;
  }

  public Map<String,String> getProperties() {
    return this.properties;
  }

  public HashingScheme getHashingScheme() {
    return this.hashingScheme;
  }

  public Integer getMaxPendingMessages() {
    return this.maxPendingMessages;
  }

  public TimeUnit getSendTimeoutUnit() {
    return this.sendTimeoutUnit;
  }

  public Integer getSendTimeoutValue() {
    return this.sendTimeoutValue;
  }

  public CompressionType getCompressionType() {
    // TODO also at the SDK level
    return this.compressionType;
  }

  public Boolean getBlockIfQueueFull() {
    return this.blockIfQueueFull;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof ProducerParams)) {
      return false;
    }

    ProducerParams t = (ProducerParams) o;

    return Objects.equals(this.getTopic(), t.getTopic()) && Objects.equals(this.getPersistent(), t.getPersistent()) && Objects.equals(this.getNamespace(), t.getNamespace()) && Objects.equals(this.getProducerName(), t.getProducerName()) && Objects.equals(this.getProperties(), t.getProperties())
      //&& Objects.equals(this.getSequenceId(), t.getSequenceId())
      ;

  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getTopic(), this.getPersistent(), this.getNamespace(), this.getProducerName()
                        //, this.getSequenceId()
                        );
  }

  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("topic", topic)
      .add("producerName", producerName)
      .toString();
  }
}