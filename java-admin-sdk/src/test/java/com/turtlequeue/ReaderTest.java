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
import java.io.FileInputStream;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import com.turtlequeue.Topic;
import java.util.concurrent.ThreadLocalRandom;
import com.turtlequeue.AcknowledgeBuilder.AckType;

import com.turtlequeue.AdminImpl; // requiring it is necessary

public class ReaderTest
{
  @Test(timeout = 20000)
  public void canReadTopicMessages()
  {
    TestConfLoader conf = new TestConfLoader();
    conf.loadConf("conf.properties");

    int r = ThreadLocalRandom.current().nextInt(1, 1001);

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
      System.out.println("Client connected " + c);

      Topic t = c.newTopicBuilder()
        .topic("testJavaSDK_Reader" + r)
        .namespace("default")
        .persistent(true)
        .build();

      AdminImpl.initialize();
      try {c.admin().deleteTopic(t, true).get(1, TimeUnit.SECONDS);} catch (Exception ex) {}

      try(Consumer consumer = c.newConsumer()
          .topic(t)
          .subscriptionName("toCreateTheTopicOnly")
          .consumerName("CREATE_TOPIC")
          .initialPosition(MessageId.earliest)
          .subscribe()
          .join();)
        {
          // hacky: make a consumer to create the topic
        }

      Producer<Object> producer = c.newProducer()
        .topic(t)
        .create()
        .get(1, TimeUnit.SECONDS);

      MessageId msgId1 = producer.newMessage().value(1).send().get(1, TimeUnit.SECONDS);
      MessageId msgId2 = producer.newMessage().value(2).send().get(1, TimeUnit.SECONDS);
      MessageId msgId3 = producer.newMessage().value(3).send().get(1, TimeUnit.SECONDS);

      Thread.sleep(200);
      Date between3And4 = new Date();
      Thread.sleep(200);

      MessageId msgId4 = producer.newMessage().value(4).send().get(1, TimeUnit.SECONDS);
      MessageId msgId5 = producer.newMessage().value(5).send().get(1, TimeUnit.SECONDS);

      Reader<byte[]> reader = c.newReader()
        .topic(t)
        .initialPosition(MessageId.earliest)
        .consumerName("READER_FOR_TOPIC")
        .create()
        .get(1, TimeUnit.SECONDS);

      Message msg1 = reader.readNext().get(1, TimeUnit.SECONDS);
      assertEquals(msgId1, msg1.getMessageId());

      Message msg2 = reader.readNext().get(1, TimeUnit.SECONDS);
      assertEquals(msgId2, msg2.getMessageId());

      Message msg3 = reader.readNext().get(1, TimeUnit.SECONDS);
      assertEquals(msgId3, msg3.getMessageId());

      Message msg4 = reader.readNext().get(1, TimeUnit.SECONDS);
      assertEquals(msgId4, msg4.getMessageId());

      Message msg5 = reader.readNext().get(1, TimeUnit.SECONDS);
      assertEquals(msgId5, msg5.getMessageId());

      // the topic has NOT been closed
      Thread.sleep(1000);
      assertEquals(false, reader.hasReachedEndOfTopic());

      // re-read from messageID 3
      // (ie. seek to messageId 2, the next message will be 3)
      reader.seek(msgId2).get(1, TimeUnit.SECONDS);

      Message msg_2_3 = reader.readNext().get(1, TimeUnit.SECONDS);
      assertEquals(msgId3, msg_2_3.getMessageId());

      Message msg_2_4 = reader.readNext().get(1, TimeUnit.SECONDS);
      assertEquals(msgId4, msg_2_4.getMessageId());

      Message msg_2_5 = reader.readNext().get(1, TimeUnit.SECONDS);
      assertEquals(msgId5, msg_2_5.getMessageId());

      // read from a Date/timestamp
      reader.seek(between3And4.getTime()).get(1, TimeUnit.SECONDS);

      Message msg_3_4 = reader.readNext().get(1, TimeUnit.SECONDS);
      assertEquals(msgId4, msg_3_4.getMessageId());

      Message msg_3_5 = reader.readNext().get(1, TimeUnit.SECONDS);
      assertEquals(msgId5, msg_3_5.getMessageId());

    } catch (Exception e) {
      System.out.println("FAIL!" + e);
      e.printStackTrace(System.out);
      fail("Should not have thrown any exception");

    }}


  // mvn -Dtest=ReaderTest#jsonPathTest -DfailIfNoTests=false test
  @Test(timeout = 20000)
  public void jsonPathTest()
  {
    TestConfLoader conf = new TestConfLoader();
    conf.loadConf("conf.properties");

    int r = ThreadLocalRandom.current().nextInt(1, 1001);

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
      System.out.println("Client connected " + c);

      Topic t = c.newTopicBuilder()
        .topic("testJavaSDK_jsonpath_Reader" + r)
        .namespace("default")
        .persistent(true)
        .build();

      AdminImpl.initialize();
      try {c.admin().deleteTopic(t, true).get(1, TimeUnit.SECONDS);} catch (Exception ex) {}

      try(Consumer consumer = c.newConsumer()
          .topic(t)
          .subscriptionName("toCreateTheTopicOnly")
          .consumerName("CREATE_TOPIC")
          .initialPosition(MessageId.earliest)
          .subscribe()
          .join();)
        {
          // make a consumer to create the topic
        }

      Producer<Object> producer = c.newProducer()
        .topic(t)
        .create()
        .get(1, TimeUnit.SECONDS);

      Map<String, String> m1 = new HashMap<String, String>() {{
          put("label", "myLabel1");
          put("image", "some_big_image");
        }};
      Map<String, String> m2 = new HashMap<String, String>() {{
          put("label", "myLabel2");
          put("image", "some_big_image");
        }};
      Map<String, String> m3 = new HashMap<String, String>() {{
          put("label", "myLabel3");
          put("image", "some_big_image");
        }};

      MessageId msgId1 = producer.newMessage().value(m1).send().get(1, TimeUnit.SECONDS);
      MessageId msgId2 = producer.newMessage().value(m2).send().get(1, TimeUnit.SECONDS);
      MessageId msgId3 = producer.newMessage().value(m3).send().get(1, TimeUnit.SECONDS);

      Reader<byte[]> reader = c.newReader()
        .topic(t)
        .initialPosition(MessageId.earliest)
        .consumerName("READER_FOR_TOPIC")
        .jsonPath("$.label") // only fetch the labels
        .create()
        .get(1, TimeUnit.SECONDS);

      Message msg1 = reader.readNext().get(1, TimeUnit.SECONDS);
      assertEquals(msgId1, msg1.getMessageId());
      assertEquals("myLabel1", msg1.getData());

      Message msg2 = reader.readNext().get(1, TimeUnit.SECONDS);
      assertEquals(msgId2, msg2.getMessageId());
      assertEquals("myLabel2", msg2.getData());

      Message msg3 = reader.readNext().get(1, TimeUnit.SECONDS);
      assertEquals(msgId3, msg3.getMessageId());
      assertEquals("myLabel3", msg3.getData());

    } catch (Exception e) {
      System.out.println("FAIL!" + e);
      e.printStackTrace(System.out);
      fail("Should not have thrown any exception");

    }}
}