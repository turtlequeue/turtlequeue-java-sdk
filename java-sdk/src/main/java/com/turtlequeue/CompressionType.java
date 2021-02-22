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

public enum CompressionType {
  None(0),
  Lz4(1),
  Zlib(2);

  private final int compressionTypeValue;

  private CompressionType(int compressionTypeValue) {
    this.compressionTypeValue = compressionTypeValue;
  }

  protected com.turtlequeue.sdk.api.proto.Tq.CompressionType toTqSubscriptionMode() {

    switch (this.compressionTypeValue) {
    case 0:
      return com.turtlequeue.sdk.api.proto.Tq.CompressionType.NONE_COMPRESSION;
    case 1:
      return com.turtlequeue.sdk.api.proto.Tq.CompressionType.LZ4;
    case 2:
      return com.turtlequeue.sdk.api.proto.Tq.CompressionType.ZLIB;
    }
    return null;
  }
}