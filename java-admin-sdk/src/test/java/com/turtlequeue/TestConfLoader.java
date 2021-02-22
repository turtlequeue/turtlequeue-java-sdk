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

public class TestConfLoader {

  Integer serverPort = null;
  String host = null;
  Boolean secure = null;

  String userToken = null;
  String apiKey = null;

  void loadConf(String configFile) {
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