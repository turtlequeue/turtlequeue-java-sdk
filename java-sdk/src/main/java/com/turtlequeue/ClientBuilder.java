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

import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.List;

import com.cognitect.transit.WriteHandler;
import com.cognitect.transit.ReadHandler;
import com.cognitect.transit.DefaultReadHandler;
import com.cognitect.transit.ArrayReader;
import com.cognitect.transit.MapReader;
import com.cognitect.transit.Reader;
import com.cognitect.transit.Writer;

import com.turtlequeue.Client;
import com.turtlequeue.ClientImpl;

/**  Internal
 *
 **/
public class ClientBuilder {
  String host = "";
  Integer port = -1;
  Boolean secure = true;
  // can be overridden = how?
  // OverrideRequest parameters object?
  // Integer defaultDeadlineMs = 2000;
  // Integer heartbeatDefaultMs = 10000;

  String apiKey = null;
  String userToken = null;

  Map<String, ReadHandler<?, ?>> customReadHandlers = null;
  Map<Class, WriteHandler<?, ?>> customWriteHandlers = null;
  DefaultReadHandler<?> customReadDefaultHandler = null;
  WriteHandler<?, ?> customDefaultWriteHandler = null;
  MapReader<?, Map<Object, Object>, Object, Object> mapBuilder = null;
  ArrayReader<?, List<Object>, Object> listBuilder = null;
  Reader transitReader = null;
  Writer transitWriter = null;

  public ClientBuilder setHost(String host) {
    this.host = host;
    return this;
  }

  public ClientBuilder setPort(Integer port) {
    this.port = port;
    return this;
  }

  public ClientBuilder setSecure(Boolean secure) {
    this.secure = secure;
    return this;
  }


  public ClientBuilder setUserToken(String userToken) {
    this.userToken = userToken;
    return this;
  }


  public ClientBuilder setApiKey(String apiKey) {
    this.apiKey = apiKey;
    return this;
  }

  public ClientBuilder transitWriteHandlers(Map<Class, WriteHandler<?, ?>> customHandlers) {
    this.customWriteHandlers = customHandlers;
    return this;
  }

  public ClientBuilder transitReadHandlers(Map<String, ReadHandler<?, ?>> customHandlers) {
    this.customReadHandlers = customHandlers;
    return this;
  }

  public ClientBuilder transitReadDefaultHandler(DefaultReadHandler<?> customDefaultHandler) {
    this.customReadDefaultHandler = customDefaultHandler;
    return this;
  }

  public ClientBuilder transitWriteDefaultHandler(WriteHandler<?, ?> defaultWriteHandler) {
    this.customDefaultWriteHandler = defaultWriteHandler;
    return this;
  }

  public ClientBuilder transitMapBuilder(MapReader<?, Map<Object, Object>, Object, Object> mapBuilder) {
    this.mapBuilder = mapBuilder;
    return this;
  }

  public ClientBuilder transitListBuilder(ArrayReader<?, List<Object>, Object> listBuilder) {
    this.listBuilder = listBuilder;
    return this;
  }

  public ClientBuilder transitReader(Reader reader) {
    this.transitReader = reader;
    return this;
  }

  public ClientBuilder transitWriter(Writer writer) {
    this.transitWriter = writer;
    return this;
  }

  public Client build() {
    return new ClientImpl(this.host, this.port, this.secure, this.userToken, this.apiKey,
                          this.transitReader,
                          this.transitWriter,
                          this.customReadHandlers,
                          this.customWriteHandlers,
                          this.customReadDefaultHandler,
                          this.customDefaultWriteHandler,
                          this.mapBuilder,
                          this.listBuilder);
  }
}