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
import java.util.concurrent.ExecutionException;
import io.grpc.Status;

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
import com.turtlequeue.AcknowledgeBuilder.AckType;

import com.turtlequeue.AdminImpl; // requiring it is necessary

import io.grpc.StatusRuntimeException;

public class ClientTest
{

  @Test
  public void smokeTest()
  {
    assertTrue( true );
  }

  @Test(timeout = 10000)
  public void canConnectSendMessages()
  {
    // - subscribe
    // - publish three messages
    // - get 1 Date + ack
    // - get 2 UUID + ack
    // - get 3 HashMap + nAck
    // - get 3 + ack

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

      int r = ThreadLocalRandom.current().nextInt(1, 1001);

      Topic t = c.newTopicBuilder()
        .topic("testJavaSDK_" + r)
        .namespace("default")
        .persistent(true)
        .build();

      AdminImpl.initialize();

      try {
        c.admin().deleteTopic(t, true) .get(1, TimeUnit.SECONDS);
      } catch (ExecutionException ex) {
        // might happen if the topic does not exist yet
        //  .get wraps the StatusRuntimeException in a java.util.concurrent.ExecutionException
        Throwable cause = ex.getCause();
        assertTrue(cause instanceof StatusRuntimeException);
      }

      Consumer consumer = c.newConsumer()
        .topic(t)
        .subscriptionName("testSub")
        .persistent(true)
        .initialPosition(MessageId.earliest)
        .receiverQueueSize(4)
        .ackTimeout(10, TimeUnit.SECONDS)
        .subscribe()
        .get(1, TimeUnit.SECONDS);

      Date msg1 = new Date();
      UUID msg2 = UUID.randomUUID();
      Map msg3 =  new HashMap();
      msg3.put("a","b");
      msg3.put("d", 89L);
      msg3.put("e", null);


      Producer<Object> producer = c.newProducer()
        .topic(t)
        .create()
        .get(1, TimeUnit.SECONDS);

      MessageId messageId1 = producer.newMessage().value(msg1).send().get(1, TimeUnit.SECONDS);
      MessageId messageId2 = producer.newMessage().value(msg2).send().get(1, TimeUnit.SECONDS);
      MessageId messageId3 = producer.newMessage().value(msg3).send().get(1, TimeUnit.SECONDS);

      // message 1: Date
      Message<Date> rcv1 = (Message<Date>) consumer.receive().get(1,  TimeUnit.SECONDS);
      Date d = rcv1.getData();

      assertEquals(messageId1, rcv1.getMessageId());
      assertEquals(msg1, rcv1.getData());

      consumer.acknowledge(rcv1).get(1,  TimeUnit.SECONDS);

      // message 2: UUID

      Message<UUID> rcv2 = (Message<UUID>) consumer.receive().get(1,  TimeUnit.SECONDS);

      assertEquals(messageId2, rcv2.getMessageId());
      assertEquals(msg2, rcv2.getData());
      rcv2.acknowledge().get(1,  TimeUnit.SECONDS);

      // message 3: Hashmap, NACK
      Message<Map> rcv3 = (Message<Map>) consumer.receive().get(1,  TimeUnit.SECONDS);

      assertEquals(messageId3, rcv3.getMessageId());
      assertTrue(Maps.difference(msg3, rcv3.getData()).areEqual());

      c.newAcknowledge()
        .setConsumer(consumer)
        .setMessage(rcv3)
        .setAckType(AckType.Individual)
        .ack()
        .get(1,  TimeUnit.SECONDS);

      try {
        c.admin().deleteTopic(t, true).get(1, TimeUnit.SECONDS);
      } catch (ExecutionException ex) {
        //  .get wraps the StatusRuntimeException in a java.util.concurrent.ExecutionException
        Throwable cause = ex.getCause();
        assertTrue(cause instanceof StatusRuntimeException);
        assertEquals(Status.Code.FAILED_PRECONDITION, ((StatusRuntimeException)cause).getStatus().getCode());
      }
      consumer.close();

      Thread.sleep(2000);
      // should NOT Error now
      c.admin().deleteTopic(t, true).get(1, TimeUnit.SECONDS);

    } catch (Exception e) {
      System.out.println("FAIL!" + e);
      e.printStackTrace(System.out);
      fail("Should not have thrown any exception");
    }
  }


}
