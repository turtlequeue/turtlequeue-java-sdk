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