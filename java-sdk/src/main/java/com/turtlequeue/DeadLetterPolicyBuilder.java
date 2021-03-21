package com.turtlequeue;

import com.turtlequeue.DeadLetterPolicy;

public class DeadLetterPolicyBuilder {

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

  public DeadLetterPolicyBuilder maxRedeliverCount(int count) {
    this.maxRedeliverCount = count;
    return this;
  }

  public DeadLetterPolicyBuilder retryLetterTopic(String topic) {
    this.retryLetterTopic = topic;
    return this;
  }

  public DeadLetterPolicyBuilder deadLetterTopic(String topic) {
    this.deadLetterTopic = topic;
    return this;
  }

  public DeadLetterPolicy build() {
    return new DeadLetterPolicy(this.maxRedeliverCount, this.retryLetterTopic, this.deadLetterTopic);
  }
}