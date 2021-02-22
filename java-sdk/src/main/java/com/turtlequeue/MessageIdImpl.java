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

import static com.google.common.base.Preconditions.checkNotNull;

import com.turtlequeue.MessageId;
import com.turtlequeue.sdk.api.proto.Tq.MessageIdData;

import com.google.common.collect.ComparisonChain;

import java.io.IOException;


public class MessageIdImpl implements MessageId {
  protected final long ledgerId;
  protected final long entryId;
  protected final int partitionIndex;

  public MessageIdImpl(long ledgerId, long entryId, int partitionIndex) {
    this.ledgerId = ledgerId;
    this.entryId = entryId;
    this.partitionIndex = partitionIndex;
  }

  public long getLedgerId() {
    return ledgerId;
  }

  public long getEntryId() {
    return entryId;
  }

  public int getPartitionIndex() {
    return partitionIndex;
  }

  @Override
  public int hashCode() {
    return (int) (31 * (ledgerId + 31 * entryId) + partitionIndex);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MessageIdImpl) {
      MessageIdImpl other = (MessageIdImpl) obj;
      return ledgerId == other.ledgerId && entryId == other.entryId && partitionIndex == other.partitionIndex;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%d:%d:%d", ledgerId, entryId, partitionIndex);
  }


  @Override
  public int compareTo(MessageId o) {
    if (o instanceof MessageIdImpl) {
      MessageIdImpl other = (MessageIdImpl) o;
      return ComparisonChain.start()
        .compare(this.ledgerId, other.ledgerId)
        .compare(this.entryId, other.entryId)
        .compare(this.getPartitionIndex(), other.getPartitionIndex())
        .result();
    } else {
      throw new IllegalArgumentException("expected MessageIdImpl object. Got instance of " + o.getClass().getName());
    }
  }

  static protected MessageIdImpl fromMessageIdData(MessageIdData msg) {
    return new MessageIdImpl(msg.getLedgerId(),
                             msg.getEntryId(),
                             msg.getPartition());

  }

  static protected MessageIdData toMessageIdData(MessageId msg) {
    return MessageIdData.newBuilder()
      .setLedgerId(msg.getLedgerId())
      .setEntryId(msg.getEntryId())
      .setPartition(msg.getPartitionIndex())
      .build();
  }
}