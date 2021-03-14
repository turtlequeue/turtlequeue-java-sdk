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

import java.util.Date;
import java.util.Map;
import java.util.HashMap;

import java.util.List;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.CompletableFuture;

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


public class MiniBenchTest
{
   // mvn -Dtest=MiniBenchTest#send100K -DfailIfNoTests=false test
  @Test(timeout = 60000)
  public void send100K()
  {
    //
    // this checks ordering implicitly
    //
    // TestConfLoader conf = new TestConfLoader();
    // conf.loadConf("conf.properties");

    // int numOfMessages = 1000;

    // try(Client c = Client.builder()
    //     .setHost(conf.getHost())
    //     .setPort(conf.getServerPort())
    //     .setSecure(conf.getSecure())
    //     .setUserToken(conf.getUserToken())
    //     .setApiKey(conf.getApiKey())
    //     .build();) {

    //   c.connect().get(1, TimeUnit.SECONDS);
    //   System.out.println("Client connected " + c);

    //   Topic t = c.newTopicBuilder()
    //     .topic("testJavaSDKMiniBench")
    //     .namespace("default")
    //     .persistent(true)
    //     .build();

    //   AdminImpl.initialize();
    //   try {
    //     c.admin().deleteTopic(t, true, true).get(1, TimeUnit.SECONDS);
    //   } catch (Exception ex) {
    //     // exists from previous test
    //   }

    //   Consumer consumer = c.newConsumer()
    //     .topic(t)
    //     .subscriptionName("testSubMiniBench")
    //     .initialPosition(MessageId.earliest)
    //     .subscribe()
    //     .get(1, TimeUnit.SECONDS);

    //   Producer producer = c.newProducer()
    //     .topic(t)
    //     // .blockIfQueueFull(true) TODO
    //     .create()
    //     .get(1, TimeUnit.SECONDS);

    //   Date [] sentTs = new Date[numOfMessages];
    //   final CompletableFuture [] recvFutTs = new CompletableFuture[numOfMessages];

    //   Date [] recvTs = new Date[numOfMessages];

    //   Runnable sendLoop =
    //     () -> {
    //     for(int i=0; i < numOfMessages ; i++) {
    //       producer.newMessage().value(i).send().exceptionally(ex -> {
    //           System.out.println("FAILED TO SEND " + ex);
    //           return null;
    //         });
    //       // date of .publish called
    //       sentTs[i] = new Date();
    //       recvFutTs[i] = new CompletableFuture<Date>();
    //     }};

    //   Runnable receiveLoop =
    //     () -> {
    //     for(int i=0 ; i < numOfMessages ; i++) {
    //       final int icpy = i;
    //       try {


    //       consumer.receive()
    //       .thenApply(arg -> {
    //           Message<Integer>  msg = (Message) arg;
    //           final Date d = new Date();
    //           recvFutTs[icpy].complete(d);
    //           assertEquals((long)icpy, (long) msg.getData());
    //           return msg;
    //         })
    //       .get();
    //       } catch (Exception ex) {
    //         System.out.println("ERROR RECEIVING " +  ex);
    //       }
    //     }};

    //   Thread sendThread = new Thread(sendLoop);
    //   Thread receiveThread = new Thread(receiveLoop);
    //   sendThread.start();
    //   receiveThread.start();
    //   sendThread.join();
    //   receiveThread.join();
    //   try {
    //     CompletableFuture.allOf(recvFutTs).get(1000 * numOfMessages, TimeUnit.MILLISECONDS);
    //   } catch (Exception ex) {
    //     System.out.println("TOO LONG, STATE WAS: ");
    //     System.out.println("sentTs:" +  Arrays.toString(sentTs));
    //     System.out.println("recvFutTs: " +  Arrays.toString(recvFutTs));
    //     throw ex;
    //   }

    //   System.out.println("DONE RECEIVING -------------------- ");

    // } catch (Exception e) {
    //   System.out.println("FAIL!" + e);
    //   e.printStackTrace(System.out);
    //   fail("Should not have thrown any exception");

    // }
  }

}