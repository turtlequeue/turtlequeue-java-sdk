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

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import com.turtlequeue.ClientImpl;
import com.turtlequeue.ClientBuilder;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.Handler;

public class TestConfLoader {

  Integer serverPort = null;
  String host = null;
  Boolean secure = null;

  String userToken = null;
  String apiKey = null;

  private static void setLevel(Level targetLevel) {
    Logger root = Logger.getLogger("");
    root.setLevel(targetLevel);
    for (Handler handler : root.getHandlers()) {
      handler.setLevel(targetLevel);
    }
    System.out.println("level set: " + targetLevel.getName());
  }

  void setupTestLog() {
    // https://www.logicbig.com/tutorials/core-java-tutorial/logging/levels.html
    System.setProperty("java.util.logging.SimpleFormatter.format",
                       // https://docs.oracle.com/javase/9/docs/api/java/util/logging/SimpleFormatter.html
                       "%1$tc %2$s%n%4$s: %5$s%6$s%n"
                       // "[%1$tF %1$tT %1$tL] [%4$-7s] %5$s %n"
                       );
    setLevel(Level.INFO);
  }

  void loadConf(String configFile) {
    setupTestLog();

    final Properties pc = new Properties();

    try (final FileInputStream fis = new FileInputStream(configFile);) {
      pc.load(fis);
    } catch (Exception ex) {
      System.out.println("Cannot open config file " + configFile + " falling back to default values");
    }

    try {
      this.serverPort = Integer.parseInt(System.getenv("TURTLEQUEUE_SERVER_PORT"));
    } catch (Exception ex) {
      // ignored, fall back to file defaults
    }
    if (this.serverPort == null) {
      this.serverPort =  Integer.parseInt(pc.getProperty("conn.serverPort", "35000"));
    }

    try {
      this.host = System.getenv("TURTLEQUEUE_HOST");
    } catch (Exception ex) {
      // ignored, fall back to file defaults
    }
    if (this.host == null ) {
      this.host = pc.getProperty("conn.host", "turtlequeue.com");
    }

    try {
      this.secure = Boolean.parseBoolean(System.getenv("TURTLEQUEUE_SECURE_CONN"));
    } catch (Exception ex) {
      // ignored, fall back to file defaults
    }
    if (this.secure == null) {
      this.secure = Boolean.parseBoolean(pc.getProperty("conn.secure", "true"));
    }

    this.userToken = System.getenv("TURTLEQUEUE_USER_TOKEN");
    // if(userToken == null ) {
    // throw?
    // }
    this.apiKey = System.getenv("TURTLEQUEUE_API_KEY");
    // if(this.apiKey == null) {
    // throw?
    // }
  }

  Integer getServerPort() { return this.serverPort; }
  String getHost() { return this.host; }
  Boolean getSecure() { return this.secure; }

  String getUserToken() { return this.userToken; }
  String getApiKey() { return this.apiKey; }
}