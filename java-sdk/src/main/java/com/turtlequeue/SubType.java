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

//
// keep in sync with GRPC enum
//
public enum SubType {
  /**
   * <code>Exclusive = 0;</code>
   */
  Exclusive(0),
  /**
   * <code>Shared = 1;</code>
   */
  Shared(1),
  /**
   * <code>Failover = 2;</code>
   */
  Failover(2),
  /**
   * <code>Key_Shared = 3;</code>
   */
  Key_Shared(3);

  // UNRECOGNIZED(-1);

  private final int subTypeValue;

  private SubType(int subTypeValue) {
    this.subTypeValue = subTypeValue;
  }

  protected Tq.SubType toTqSubType() {
    switch (this.subTypeValue) {
    case 0:
      return com.turtlequeue.sdk.api.proto.Tq.SubType.EXCLUSIVE_SUB;
    case 1:
      return com.turtlequeue.sdk.api.proto.Tq.SubType.SHARED_SUB;
    case 2:
      return com.turtlequeue.sdk.api.proto.Tq.SubType.FAILOVER_SUB;
    case 3:
      return com.turtlequeue.sdk.api.proto.Tq.SubType.KEY_SHARED_SUB;
    }
    // or throw?
    return null;
  }

}
