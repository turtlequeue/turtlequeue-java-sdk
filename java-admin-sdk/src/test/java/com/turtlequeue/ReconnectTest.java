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
import com.turtlequeue.AcknowledgeBuilder.AckType;

import com.turtlequeue.AdminImpl;

public class ReconnectTest
{
  // @Test(timeout = 10000)
  // public void closeAndReopen()
  // {
  //   // checking the channel close/reopen
  //   TestConfLoader conf = new TestConfLoader();
  //   conf.loadConf("conf.properties");

  //   AdminImpl.initialize();

  //   int times = 3;
  //   for(int i = 0; i < times; i++) {
  //     try(
  //         Client c = Client.builder()
  //         .setHost(conf.getHost())
  //         .setPort(conf.getServerPort())
  //         .setSecure(conf.getSecure())
  //         .setUserToken(conf.getUserToken())
  //         .setApiKey(conf.getApiKey())
  //         .build();
  //         ) {

  //       c.connect().get(1, TimeUnit.SECONDS);

  //       Topic t = c.newTopicBuilder()
  //         .topic("testJavaSDK_reconnectTest_" + i)
  //         .namespace("default")
  //         .persistent(true)
  //         .build();

  //       try {c.admin().deleteTopic(t, true).get(1, TimeUnit.SECONDS);} catch (Exception ex) {}

  //       Thread.sleep(1000);
  //       System.out.println("waited..");

  //     } catch (Exception ex) {
  //       System.out.println("FAIL!" + ex);
  //       ex.printStackTrace(System.out);
  //       fail("Should not have thrown any exception " + i);
  //     }}
  // }
}