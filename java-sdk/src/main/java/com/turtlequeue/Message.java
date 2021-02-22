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
import java.util.concurrent.TimeUnit;

import com.turtlequeue.MessageId;
import java.util.concurrent.CompletableFuture;
import com.turtlequeue.Producer;

public interface Message <T> {
  public Client getClient();
  public T getData(); // TODO make a test with byte[]
  public Map<String,String> getProperties();
  public String getProducerName();
  public Long getEventTime();
  public Topic getTopic();
  public String getKey();

  // consumer
  public Consumer<T> getConsumer();
  public MessageId getMessageId();
  public CompletableFuture<Void> acknowledge();
  public Long getPublishTime();

  // producer
  public Producer<T> getProducer();
  public boolean isReplicated();  // symetry isReplicationDisabled
  public String getReplicatedFrom();
  public TimeUnit getDelayTimeUnit();
  public Long getDelayValue();

}