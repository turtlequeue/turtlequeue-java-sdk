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

import java.util.Objects;

import com.google.common.base.MoreObjects;

import com.turtlequeue.Topic;
import com.turtlequeue.sdk.api.proto.Tq;

// immutable Topic

public class TopicImpl implements Topic {
  String topic = null;
  String namespace = null;
  Boolean persistent = null;

  public TopicImpl(String topic, String namespace, boolean persistent) {
    this.topic = topic;
    this.namespace = namespace;
    this.persistent = persistent;
  }

  public String getTopic() {
    return this.topic;
  }

  public String getNamespace() {
    return this.namespace;
  }

  public boolean getPersistent() {
    return this.persistent;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof Topic)) {
      return false;
    }

    Topic t = (Topic) o;

    return Objects.equals(this.getTopic(), t.getTopic()) && Objects.equals(this.getNamespace(), t.getNamespace()) && Objects.equals(this.getPersistent(), t.getPersistent());

  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getTopic(), this.getNamespace(), this.getPersistent());
  }


  @Override()
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("topic", this.topic)
      .add("persistent", this.persistent)
      .add("namespace", this.namespace)
      .toString();
  }

  protected static Topic fromTqTopic(Tq.Topic t) {
    return new TopicBuilderImpl()
      .namespace(t.getNamespace())
      .persistent(t.getPersistent())
      .topic(t.getTopic())
      .build();
  }

  protected static Tq.Topic toTqTopic(Topic t) {
    return Tq.Topic.newBuilder()
      .setNamespace(t.getNamespace())
      .setPersistent(t.getPersistent())
      .setTopic(t.getTopic())
      .build();
  }
}
