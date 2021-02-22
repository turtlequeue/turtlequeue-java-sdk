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


import java.io.IOException;
import java.io.Serializable;

import java.util.stream.Stream;
import java.util.stream.LongStream;
import java.util.stream.Collectors;
import java.util.List;

import com.turtlequeue.sdk.api.proto.Tq.MessageIdData;

/**
 * Opaque unique identifier of a single message
 *
 * <p>The MessageId can be used to reference a specific message, for example when acknowledging, without having
 * to retain the message content in memory for an extended period of time.
 *
 * <p>Message ids are {@link Comparable} and a bigger message id will imply that a message was published "after"
 * the other one.
 */
public interface MessageId extends Comparable<MessageId>
                                   // TODO
                                   // , Serializable
{

  /**
   * MessageId that represents the oldest message available in the topic.
   */
  MessageId earliest = new MessageIdImpl(-1, -1, -1);

  /**
   * MessageId that represents the next message published in the topic.
   */
  MessageId latest = new MessageIdImpl(Long.MAX_VALUE, Long.MAX_VALUE, -1);

  static public MessageId fromMessageIdData(MessageIdData msg) {
    return new MessageIdImpl(msg.getLedgerId(), msg.getEntryId(), msg.getPartition());
  }

  // static public MessageId fromString(String messageStr) {
  //   String[] str = messageStr.split(":");
  //   // TODO add checks?
  //   long ledgerId = Long.parseLong(str[0]);
  //   long entryId = Long.parseLong(str[1]);
  //   int partitionIndex = Integer.parseInt(str[2]);

  //   return new MessageIdImpl(ledgerId, entryId, partitionIndex);
  // }

  public long getLedgerId();
  public long getEntryId();
  public int getPartitionIndex();

}
