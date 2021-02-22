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
import java.util.concurrent.TimeUnit;
import java.io.FileInputStream;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.CompletableFuture;

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
import com.turtlequeue.SubInitialPosition;
import com.turtlequeue.AcknowledgeBuilder.AckType; // TODO move to own class?

import com.turtlequeue.AdminImpl; // requiring it is necessary

public class NackTest
{

  @Test(timeout = 10000)
  public void canConnectSendMessages()
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
        .build();
        ) {

      c.connect().get(1, TimeUnit.SECONDS);

      System.out.println("Client connected: " + c);

      int r = ThreadLocalRandom.current().nextInt(1, 1001);

      Topic t = c.newTopicBuilder()
        .topic("testJavaSDK_Nack" + r)
        .namespace("default")
        .persistent(true)
        .build();

      AdminImpl.initialize();
      try {
        c.admin().deleteTopic(t, true).get(1, TimeUnit.SECONDS);
      } catch (Exception ex) {
        // not found or already has producers/consumers -> ignore
      }

      Consumer<Date> consumer = c.newConsumer()
        .topic(t)
        .subscriptionName("testSub")
        .persistent(true)
        .initialPosition(MessageId.earliest)
        .receiverQueueSize(4)
        .ackTimeout(10, TimeUnit.SECONDS)
        .subscribe()
        .get(1, TimeUnit.SECONDS);

      Date msg1 = new Date();

      Producer<Date> producer = c.newProducer()
        .topic(t)
        .create()
        .get(1, TimeUnit.SECONDS);

      MessageId messageId1 = producer.newMessage()
        .value(msg1)
        .send()
        .get(1, TimeUnit.SECONDS);

      // message 1: Date
      Message<Date> rcv1 = consumer.receive().get(1,  TimeUnit.SECONDS);
      Date d = rcv1.getData();

      assertEquals(messageId1, rcv1.getMessageId());
      assertEquals(msg1, rcv1.getData());

      System.out.println("ABOUT TO ACK NOW ");

      consumer.nonAcknowledge(rcv1).get(1,  TimeUnit.SECONDS);

      // force redelivery of un-acknowledged messages (avoids waiting 10 seconds)
      consumer.redeliverUnacknowledgedMessages().get(1, TimeUnit.SECONDS);

      // message 4: redelivery of the above message
      System.out.println("Waiting for re-delivery...");

      Message<Date> rcv2 = consumer.receive().get(1,  TimeUnit.SECONDS);

      assertEquals(messageId1, rcv1.getMessageId());
      rcv2.acknowledge().get(1, TimeUnit.SECONDS);

      // cleanup topic
      c.admin().deleteTopic(t, true).get(1, TimeUnit.SECONDS);

    } catch (Exception e) {
      System.out.println("FAIL!" + e);
      e.printStackTrace(System.out);
      fail("NackTest: Exception FAIL");
    }}


}
