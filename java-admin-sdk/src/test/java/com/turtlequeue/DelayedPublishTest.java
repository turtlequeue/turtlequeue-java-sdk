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

import java.util.Properties;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.Queue;
import java.util.List;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Maps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

import com.turtlequeue.TestConfLoader;

// TurtleQueue public imports
import com.turtlequeue.ClientImpl;
import com.turtlequeue.ClientBuilder;
import com.turtlequeue.MessageId;
import com.turtlequeue.SubType;
import com.turtlequeue.Topic;
import com.turtlequeue.Message;
import com.turtlequeue.SubInitialPosition;
import com.turtlequeue.AcknowledgeBuilder.AckType;

import com.turtlequeue.AdminImpl; // requiring it is necessary

// mvn -Dtest=DelayedPublishTest -DfailIfNoTests=false test
public class DelayedPublishTest
{

  @Test(timeout = 20000)
  public void sendDelayedMessages()
  {
    TestConfLoader conf = new TestConfLoader();
    conf.loadConf("conf.properties");

    try(
        Client c = Client.builder()
        .setHost(conf.getHost())
        .setPort(conf.getServerPort())
        .setSecure(conf.getSecure())
        .setUserToken(conf.getUserToken())
        .setApiKey(conf.getApiKey())
        .build()
        .connect()
        .get(1, TimeUnit.SECONDS);
        ) {

      System.out.println("Client connected " + c);

      Topic t = c.newTopicBuilder()
        .topic("testJavaSDKDelayedPublishing" + ThreadLocalRandom.current().nextInt(0, 10000))
        .namespace("default")
        .persistent(true)
        .build();

      AdminImpl.initialize();

      try {
        c.admin().deleteTopic(t, true).get(1, TimeUnit.SECONDS);
      } catch (Exception ex) {
        // ignore, topic may not exist yet
      }

      Consumer<String> consumer = c.newConsumer()
        .topic(t)
        .subscriptionName("testJavaSDKDelayedConsumer")
        // delays only work on shared subscriptions
        .subscriptionType(SubType.Shared)
        .initialPosition(MessageId.earliest)
        .receiverQueueSize(1)
        .subscribe()
        .get(1, TimeUnit.SECONDS);

      Producer<String> producer = c.newProducer()
        .topic(t)
        .create()
        .get(1, TimeUnit.SECONDS);

      Long eventTime = System.currentTimeMillis();

      MessageId msgId1 = producer.newMessage()
        .deliverAfter(10L, TimeUnit.SECONDS)
        .eventTime(eventTime)
        .value("I will be delivered in 10 seconds")
        .send()
        .get(1, TimeUnit.SECONDS);

        // .publish(c.publishParams()
               //                     .setTopic(t)
               //                     .deliverAfter(10L, TimeUnit.SECONDS)
               //                     .setEventTime(eventTime)
               //                     .setPayload("I will be delivered in 10 seconds")
               //                     .create()).get(1, TimeUnit.SECONDS);

      CompletableFuture f = consumer.receive();
      try {
        f.get(9, TimeUnit.SECONDS);
        fail("Should have thrown a TimeoutException");
      } catch (TimeoutException expectedEx) {
        f.cancel(true);
      }

      Message msg1 = consumer.receive().get(3, TimeUnit.SECONDS);
      assertEquals(msgId1, msg1.getMessageId());
      assertEquals((Long) eventTime, (Long) msg1.getEventTime());

      // get published time
    } catch (Exception e) {
      System.out.println("FAIL!" + e);
      e.printStackTrace(System.out);
      fail("Should not have thrown any exception");

    }}

}