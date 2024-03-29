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
import com.turtlequeue.Topic;
import com.turtlequeue.SubInitialPosition;
import com.turtlequeue.AcknowledgeBuilder.AckType;

import com.turtlequeue.AdminImpl; // requiring it is necessary

public class ClientOneToOneTest
{

  @Test(timeout = 10000)
  public void sendNSmallMessages()
  {
    //
    // this checks ordering implicitly
    //
    TestConfLoader conf = new TestConfLoader();
    conf.loadConf("conf.properties");

    int numOfMessages = 10;

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

      System.out.println("Client connected ");

      Topic t = c.newTopicBuilder()
        .topic("testJavaSDKOneToOneOrdering")
        .namespace("default")
        .persistent(true)
        .build();

      AdminImpl.initialize();
      try {
        c.admin().deleteTopic(t, true, true).get(1, TimeUnit.SECONDS);
      } catch (Exception ex) {
        // exists from previous test
      }

      Consumer consumer = c.newConsumer()
        .topic(t)
        .subscriptionName("testSubOneToOne")
        .initialPosition(MessageId.earliest)
        .receiverQueueSize(3) // small number forces flow messages
        .subscribe()
        .get(1, TimeUnit.SECONDS);

      Producer producer = c.newProducer()
        .topic(t)
        .create()
        .get(1, TimeUnit.SECONDS);

      Date [] sentTs = new Date[numOfMessages];
      final CompletableFuture [] recvFutTs = new CompletableFuture[numOfMessages];

      Date [] recvTs = new Date[numOfMessages];


      for(int i=0; i < numOfMessages ; i++) {

        producer.newMessage()
          .key("my-message-key")
          .value(i)
          .property("my-key", "my-value")
          .property("my-other-key", "my-other-value")
          .send();

        // date of .publish called
        sentTs[i] = new Date();
        recvFutTs[i] = new CompletableFuture<Date>();
       }

      System.out.println("DONE SENDING -------------------- ");

      for(int i=0 ; i < numOfMessages ; i++) {
        final int icpy = i;
        consumer.receive()
          .thenApply(arg -> {
              Message msg = (Message) arg;
              //System.out.println("RECEIVED: recvFutTs=" +  Arrays.toString(recvFutTs));
              final Date d = new Date();
              recvFutTs[icpy].complete(d);
              assertEquals(icpy, msg.getData());
              return msg;
            });
      }

      // give it some time because the small receiver queue means 1 flow exchange every time
      try {
        CompletableFuture.allOf(recvFutTs).get(500 * numOfMessages, TimeUnit.MILLISECONDS);
        System.out.println("DONE RECEIVING -------------------- ");

      } catch (Exception ex) {
        System.out.println("TOO LONG, STATE WAS: ");
        System.out.println("sentTs:" +  Arrays.toString(sentTs));
        System.out.println("recvFutTs: " +  Arrays.toString(recvFutTs));
        throw ex;
      }
      System.out.println("DONE RECEIVING -------------------- ");

    } catch (Exception e) {
      System.out.println("FAIL!" + e);
      e.printStackTrace(System.out);
      fail("Should not have thrown any exception");

    }}
}