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

import java.io.Serializable;

import com.turtlequeue.Consumer;

/**
 * A listener that will be called when the end of the topic has been reached.
 */
public interface EndOfTopicMessageListener<T> extends Serializable {

  /**
   * Get the notification when a topic is terminated.
   *
   * @param consumer
   *            the Consumer object associated with the terminated topic
   */
  default void reachedEndOfTopic(Consumer<T> consumer) {
    // By default ignore the notification
  }
}
