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

// import java.util.Map;
// import java.util.Objects;
// import java.util.concurrent.CompletableFuture;

// import com.google.common.base.MoreObjects;

// import com.turtlequeue.Message;
// import com.turtlequeue.ClientImpl;
// import com.turtlequeue.MessageIdImpl;

// public class ProducerMessageImpl<T> implements Message, ConsumerMessage {
//   ClientImpl c = null;
//   ConsumerImpl consumer = null;
//   MessageId messageId = null;
//   // Object? or parametrized <T> ?
//   T data = null;
//   String producerName = null;
//   Long eventTime = null;
//   Long publishTime = null;
//   Topic topic = null;
//   String key = null;
//   Map<String,String> properties = null;

//   public ProducerMessageImpl (ClientImpl c, ConsumerImpl consumer, MessageIdImpl messageId, T payload, String producerName, long eventTime, Long publishTime, Topic topic, String key, Map<String, String> properties) {
//     this.c = c;
//     this.consumer = consumer;
//     this.messageId = messageId;
//     this.data = payload;
//     this.producerName = producerName;
//     this.eventTime = eventTime;
//     this.publishTime = publishTime;
//     this.topic = topic;
//     this.key = key;
//     this.properties = properties;
//   }

//   public MessageId getMessageId() {
//     return this.messageId;
//   }

//   public T getData() {
//     return this.data;
//   }

//   public Consumer getConsumer() {
//     return this.consumer;
//   }

//   public Client getClient() {
//     return this.c;
//   }

//   public String getProducerName() {
//     return this.producerName;
//   }

//   public long getEventTime() {
//     return this.eventTime;
//   }

//   public Long getPublishTime() {
//     return this.publishTime;
//   }

//   public Topic getTopic() {
//     if(this.topic != null && this.topic.getTopic() != null) {
//       return this.topic;
//     } else if (this.consumer != null) {
//       return this.consumer.getTopic();
//     }
//     return null;
//   }

//   public Map<String,String> getProperties() {
//     return this.properties;
//   }

//   public String getKey() {
//     return this.key;
//   }

//   public CompletableFuture<Void> acknowledge() {
//     return this.consumer.acknowledge(this);
//   }

//   @Override
//   public boolean equals(Object o) {
//     if (o == this) {
//       return true;
//     }

//     if (!(o instanceof Message)) {
//       return false;
//     }

//     Message t = (Message) o;

//     return Objects.equals(this.getTopic(), t.getTopic()) && Objects.equals(this.getMessageId(), t.getMessageId()) && Objects.equals(this.getProducerName(), t.getProducerName()) && Objects.equals(this.getEventTime(), t.getEventTime()) && Objects.equals(this.getPublishTime(), t.getPublishTime()) && Objects.equals(this.getTopic(), t.getTopic()) && Objects.equals(this.getKey(), t.getKey()) && Objects.equals(this.getProperties(), t.getProperties());
//   }

//   @Override
//   public int hashCode() {
//     return Objects.hash(this.getTopic(), this.getProducerName(), this.getMessageId(), this.getEventTime(), this.getKey(), this.getPublishTime());
//   }

//   public String toString() {
//     return MoreObjects.toStringHelper(this)
//       .add("topic", topic)
//       .add("producerName", producerName)
//       //.add("data", data) // TODO hide? truncate? Type? Size?
//       .add("eventTime", eventTime)
//       .add("publishTime", publishTime)
//       .add("key", key)
//       .toString();
//   }
// }