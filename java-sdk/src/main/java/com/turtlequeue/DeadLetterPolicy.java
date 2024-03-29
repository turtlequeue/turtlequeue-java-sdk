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

import com.turtlequeue.DeadLetterPolicyBuilder;

public class DeadLetterPolicy {

  static DeadLetterPolicyBuilder builder() {
    return new DeadLetterPolicyBuilder ();
  }

  /**
   * Maximum number of times that a message will be redelivered before being sent to the dead letter queue.
   */
  private int maxRedeliverCount;

  /**
   * Name of the retry topic where the failing messages will be sent.
   */
  private String retryLetterTopic;

  /**
   * Name of the dead topic where the failing messages will be sent.
   */
  private String deadLetterTopic;

  public DeadLetterPolicy (int maxRedeliverCount, String retryLetterTopic,  String deadLetterTopic) {
    this.maxRedeliverCount = maxRedeliverCount;
    this.retryLetterTopic = retryLetterTopic;
    this.deadLetterTopic = deadLetterTopic;
  }

  protected int getMaxRedeliverCount() {
    return maxRedeliverCount;
  }

  protected String getRetryLetterTopic() {
    return retryLetterTopic;
  }

  protected String getDeadLetterTopic() {
    return deadLetterTopic;
  }

}