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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.InputStream;

import com.turtlequeue.sdk.api.proto.TurtleQueueGrpc;

// https://blog.soebes.de/blog/2014/01/02/version-information-into-your-appas-with-maven/

public class Version
{
  private static final Logger logger = Logger.getLogger(TurtleQueueGrpc.class.getName());

  public Version() {
  }

  static public Properties getVersion() {

    Properties defaultProps = new Properties();
    defaultProps.setProperty("version", "dev");
    defaultProps.setProperty("artifactId", "java-sdk");
    defaultProps.setProperty("groupId", "com.turtlequeue");

    InputStream resourceAsStream = ClassLoader.getSystemClassLoader().getResourceAsStream("version.properties");
    if(resourceAsStream == null ){
      logger.log(Level.WARNING, "Could not find version.properties resource file");
      return defaultProps;
    } else {
      Properties prop = new Properties(defaultProps);
      try {
        prop.load(resourceAsStream);
        return prop;
      } catch (Exception ex) {
        logger.log(Level.SEVERE, "Invalid properties format" + ex);
        return defaultProps;
      }
    }
  }
}