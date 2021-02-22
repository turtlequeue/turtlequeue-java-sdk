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

import com.turtlequeue.Topic;
import com.turtlequeue.TopicImpl;

public class TopicBuilderImpl {

  String topic = null;
  String namespace = null;
  Boolean persistent = null;

  public TopicBuilderImpl() {

  }

  public TopicBuilderImpl topic(String topic) {
    this.topic = topic;
    return this;
  }

  public TopicBuilderImpl topic(Topic topic) {
    this.topic = topic.getTopic();
    this.namespace = topic.getNamespace();
    this.persistent = topic.getPersistent();
    return this;
  }

  public TopicBuilderImpl namespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  public TopicBuilderImpl persistent(boolean persistent) {
    this.persistent = persistent;
    return this;
  }

  public Topic build() {
    if(this.topic == null) {
      throw new IllegalArgumentException("topic must be specified on the TopicBuilderImpl object.");
    }

    if(this.namespace == null) {
      throw new IllegalArgumentException("namespace must be specified on the TopicBuilderImpl object.");
    }


    if(this.persistent == null) {
      throw new IllegalArgumentException("persistent must be specified on the TopicBuilderImpl object.");
    }

    return new TopicImpl(this.topic, this.namespace, this.persistent);
  }
}