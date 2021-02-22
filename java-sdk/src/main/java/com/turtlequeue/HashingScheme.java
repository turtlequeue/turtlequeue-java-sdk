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

import com.turtlequeue.sdk.api.proto.Tq;

public enum HashingScheme {
  JavaString(0),
  MurmurHash(1);

  private final int hashTypeValue;

  private HashingScheme(int hashTypeValue) {
    this.hashTypeValue = hashTypeValue;
  }

  protected com.turtlequeue.sdk.api.proto.Tq.CommandProducerCreate.HashingScheme toTqHashingScheme() {
    switch (this.hashTypeValue) {
    case 0:
      return com.turtlequeue.sdk.api.proto.Tq.CommandProducerCreate.HashingScheme.JAVA_STRING_HASH;

    case 1:
      return com.turtlequeue.sdk.api.proto.Tq.CommandProducerCreate.HashingScheme.MURMUR3_32HASH;

    default:
      return null;
    }
  }
}