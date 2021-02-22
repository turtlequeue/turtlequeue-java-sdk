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
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Maps;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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
import com.turtlequeue.EndOfTopicMessageListener;
import com.turtlequeue.Topic;
import com.turtlequeue.AcknowledgeBuilder.AckType;
import com.turtlequeue.TqClientException;

import com.turtlequeue.AdminImpl; // requiring it is necessary

public class EndOfTopicTest
{

  // scenario
  // - create a topic + consumer
  // - send 3 messages to it
  // - close the topic via admin
  // - try to publish, check it throws
  // - .receive the 3 messages
  // - check we can't .receive a 4th
  // - check the end of topic flag was set
  @Test(timeout = 10000)
  public void receivesEndOfTopic()
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
      System.out.println("Client connected " + c);

      Topic t = c.newTopicBuilder()
        .topic("testJavaSDK_EndOfTopicTest" + ThreadLocalRandom.current().nextInt(0, 10000))
        .namespace("default")
        .persistent(true)
        .build();

      AdminImpl.initialize();
      try {c.admin().deleteTopic(t, true).get(1, TimeUnit.SECONDS);} catch (Exception ex) {}

      CompletableFuture<Void> didReachEndOfTopic = new CompletableFuture();
      Consumer<Integer> consumer = c.newConsumer()
        .topic(t)
        .subscriptionName("testSubEndOfTopic")
        .initialPosition(MessageId.earliest)
        .endOfTopicMessageListener(new EndOfTopicMessageListener(){
            @Override
            public void reachedEndOfTopic(Consumer consumer) {
              didReachEndOfTopic.complete(null);
            }})
        .subscribe()
        .get(1, TimeUnit.SECONDS);

      Producer<Object> producer = c.newProducer().topic(t).create().get(1, TimeUnit.SECONDS);

      MessageId msgId1 = producer.newMessage().value("msg1").send().get(1, TimeUnit.SECONDS);
      MessageId msgId2 = producer.newMessage().value("msg2").send().get(1, TimeUnit.SECONDS);
      MessageId msgId3 = producer.newMessage().value("msg3").send().get(1, TimeUnit.SECONDS);

      MessageId last = c.admin().terminateTopic(t).get(1, TimeUnit.SECONDS);

      assertEquals(last, msgId3);


      try {
        // Publishing on a closed topic
        producer.newMessage().value(123).send().get(1, TimeUnit.SECONDS);
        fail("Publishing on a closed topic should throw");
      }
      /* catch (TqClientException.TopicTerminatedException ex) { */
      /*   // FIXME */
      /*   // newMessage */
      /*   System.out.println("SPECIFIC EXCEPTION " + ex); */
      /*   // expected */
      /*   // TODO throw SPECIFIC EXCEPTION */
      /*   // TODO https://www.baeldung.com/java-sneaky-throws */
      /*   // */
      /* } */
      catch (Exception ex) {
        System.out.println("SPECIFIC EXCEPTION " + ex);
        System.out.println("SPECIFIC EXCEPTION CLASS" + ex.getClass().getName());

        /* SPECIFIC EXCEPTION java.util.concurrent.ExecutionException: com.turtlequeue.TqClientException$TopicTerminatedException: Topic was already terminated */
        /*   SPECIFIC EXCEPTION CLASSjava.util.concurrent.ExecutionException */

        // fail("Publishing on a closed topic should throw a specific Exception");
      }

      Message msg1 = consumer.receive().get(1, TimeUnit.SECONDS);
      //System.out.println("msg1 " + msg1.getData());
      assertEquals(msg1.getMessageId(), msgId1);
      msg1.acknowledge().get(1, TimeUnit.SECONDS);

      Message msg2 = consumer.receive().get(1, TimeUnit.SECONDS);
      //System.out.println("msg2 " + msg2.getData());
      assertEquals(msg2.getMessageId(), msgId2);
      msg2.acknowledge().get(1, TimeUnit.SECONDS);

      Message msg3 = consumer.receive().get(1, TimeUnit.SECONDS);
      //System.out.println("msg3 " + msg3.getData());
      msg3.acknowledge().get(1, TimeUnit.SECONDS);
      assertEquals(msg3.getMessageId(), msgId3);

      try {
        Message msg4 = consumer.receive().get(1, TimeUnit.SECONDS);
        // TODO which one is it? AssertNull or catch Exception?
        assertNull(msg4);
      } catch (Exception ex) {
          // this throws TimeoutException, like pulsar..
          // could be changed if pulsar threw too TODO raise it to pulsar?
      }

      didReachEndOfTopic.get(1, TimeUnit.SECONDS);


    } catch (Exception e) {
      System.out.println("FAIL!" + e);
      e.printStackTrace(System.out);
      fail("EndOfTopicTest should not have thrown");
    }}
}
