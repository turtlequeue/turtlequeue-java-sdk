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
import java.util.concurrent.TimeUnit;

import com.turtlequeue.Message;
import com.turtlequeue.MessageId;

public interface Consumer<T> extends Closeable {
  public CompletableFuture<Message<T>> receive();
  // public CompletableFuture<Void> acknowledge(MessageId messageId);
  public CompletableFuture<Void> acknowledge(Message<T> message);
  public CompletableFuture<Void> nonAcknowledge(MessageId messageId);
  // TODO comment for tracking?
  // this allows stuffing .parentMessage []
  // or other tracking info which could be passed via headers
  public CompletableFuture<Void> nonAcknowledge(Message<T> message);
  public CompletableFuture<Void> redeliverUnacknowledgedMessages();
  public CompletableFuture<Void> acknowledgeCumulativeAsync(Message<T> message);

  public boolean isConnected();
  public Boolean hasReachedEndOfTopic();
  public void close();
}
