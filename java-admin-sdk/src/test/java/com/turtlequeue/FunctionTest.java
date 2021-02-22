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

// mvn -Dtest=FunctionTest -DfailIfNoTests=false test
public class FunctionTest
{

  @Test(timeout = 20000)
  public void setAFunction()
  {
  //   TestConfLoader conf = new TestConfLoader();
  //   conf.loadConf("conf.properties");

  //   ClassLoader classLoader = getClass().getClassLoader();
  //   File file = new File(classLoader.getResource("file/ContextFunction.java.txt").getFile());
  //   // java context example from https://pulsar.apache.org/docs/en/functions-develop/
  //   String myJavaClass = Files.readString(file);

  //   try(
  //       Client c = Client.builder()
  //       .setHost(conf.getHost())
  //       .setPort(conf.getServerPort())
  //       .setSecure(conf.getSecure())
  //       .setUserToken(conf.getUserToken())
  //       .setApiKey(conf.getApiKey())
  //       .build();
  //       ) {

  //     c.connect().get(1, TimeUnit.SECONDS);
  //     System.out.println("Client connected " + c);

  //     // tIn -> Function -> tOut
  //     Topic tIn = c.newTopicBuilder()
  //       .setTopic("testJavaSDK_FunctionTestIN" + ThreadLocalRandom.current().nextInt(0, 10000))
  //       .setNamespace("default")
  //       .setPersistent(true)
  //       .build();

  //     Topic tOut = c.newTopicBuilder()
  //       .setTopic("testJavaSDK_FunctionTestOUT" + ThreadLocalRandom.current().nextInt(0, 10000))
  //       .setNamespace("default")
  //       .setPersistent(true)
  //       .build();


  //     AdminImpl.initialize();
  //     c.admin().createTopic(tIn, true).get(1, TimeUnit.SECONDS);
  //     c.admin().createTopic(tOut, true).get(1, TimeUnit.SECONDS);

  //     c.admin.createFunctionFromString()
  //       .setLanguage(Language.Java)
  //       .setClassName()
  //       .setLogTopic()
  //       .setTopicIn()
  //       .create();


  //   } catch (Exception e) {
  //     System.out.println("FAIL!" + e);
  //     e.printStackTrace(System.out);
  //     fail("Should not have thrown any exception");

  //   }
   }

}