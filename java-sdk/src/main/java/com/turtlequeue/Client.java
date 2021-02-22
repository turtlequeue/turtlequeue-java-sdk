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
import com.turtlequeue.ClientImpl;
import com.turtlequeue.ClientBuilder;
import com.turtlequeue.ConsumerParams;
import com.turtlequeue.ConsumerBuilder;
import com.turtlequeue.SubscribeResponse;
import com.turtlequeue.sdk.api.proto.Tq.BrokerToClient;
import com.turtlequeue.Consumer;
import com.turtlequeue.TopicBuilderImpl;
import com.turtlequeue.ProducerBuilder;
import com.turtlequeue.ReaderBuilder;
import com.turtlequeue.AcknowledgeBuilder;

public interface Client extends AutoCloseable {

  /**
   * Get a new builder instance that can used to configure and build a {@link Client} instance.
   *
   * @return the {@link ClientBuilder}
   *
   * @since 1.0.0
   */
  static ClientBuilder builder() {
    return new ClientBuilder ();
  }

  public CompletableFuture<Client> connect() throws Exception ;


  public ConsumerBuilder newConsumer();
  public ReaderBuilder newReader();

  public AcknowledgeBuilder newAcknowledge();

  public ProducerBuilder newProducer();

  // .close instead
  // public CompletableFuture<Void> disconnect();
  //

  public Admin admin();
  public TopicBuilderImpl newTopicBuilder();

}
