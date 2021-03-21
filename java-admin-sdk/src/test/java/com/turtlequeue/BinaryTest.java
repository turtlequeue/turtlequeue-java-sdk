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
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

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

public class BinaryTest
// test that the redelivery count is incremented
{

  @Test(timeout = 10000)
  public void redeliveryCountIsIncremented()
  {
    //
    // this checks ordering implicitly
    //
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
        .dataFormat("application/octet-stream")
        .build();) {

      c.connect().get(1, TimeUnit.SECONDS);
      System.out.println("Client connected " + c);

      Topic t = c.newTopicBuilder()
        .topic("testJavaSDKbinaryFormat" + r)
        .namespace("default")
        .persistent(true)
        .build();

      AdminImpl.initialize();
      try {
        c.admin().deleteTopic(t, true, true).get(1, TimeUnit.SECONDS);
      } catch (Exception ex) {
        // exists from previous test
      }

      Consumer<byte[]> consumer = c.newConsumer()
        .topic(t)
        .subscriptionName("my-sub")
        .subscribe()
        .get(1, TimeUnit.SECONDS);

      Producer<byte[]> producer = c.newProducer()
        .topic(t)
        .create()
        .get(1, TimeUnit.SECONDS);

      String value = "hello BINARY";

      producer.newMessage().value(value.getBytes()).send();

      Message msg = consumer.receive(1L, TimeUnit.SECONDS);
      byte[] data = (byte[]) msg.getData();
      String dataString = new String(data);
      assertEquals(value, dataString);

    } catch (Exception e) {
      System.out.println("FAIL!" + e);
      e.printStackTrace(System.out);
      fail("Should not have thrown any exception");
    }}

}