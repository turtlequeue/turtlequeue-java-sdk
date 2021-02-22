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
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import com.turtlequeue.Client;
import com.turtlequeue.ClientImpl;
import com.turtlequeue.ConsumerParams;
import com.turtlequeue.SubType;
import com.turtlequeue.ConsumerImpl;
import com.turtlequeue.Consumer;
import com.turtlequeue.Topic;
import com.turtlequeue.ReaderImpl;

public class ReaderParams<T> {

  ClientImpl c = null;
  ConsumerParams conf = null;

  public ReaderParams(ClientImpl c, ConsumerParams conf) {
    this.c = c;
    this.conf = conf;
  }

  public CompletableFuture<Reader<T>> create() {
    return (CompletableFuture<Reader<T>>) (CompletableFuture<?>) new ReaderImpl<T>(this.c, this.conf).subscribeReturn();
  }

}