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

import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.IntUnaryOperator;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import io.grpc.ConnectivityState;

import com.cognitect.transit.TransitFactory;
import com.cognitect.transit.Reader;
import com.cognitect.transit.Writer;

import com.google.common.collect.Queues;
import com.google.common.base.MoreObjects;

import com.turtlequeue.sdk.api.proto.TurtleQueueGrpc;
import com.turtlequeue.sdk.api.proto.Tq;
import com.turtlequeue.sdk.api.proto.Tq.CommandSubscribe;
import com.turtlequeue.sdk.api.proto.Tq.CommandFlow;
import com.turtlequeue.sdk.api.proto.Tq.CommandMessage;
import com.turtlequeue.sdk.api.proto.Tq.CommandConsumer;
import com.turtlequeue.sdk.api.proto.Tq.CommandAck;
import com.turtlequeue.sdk.api.proto.Tq.CommandRedeliverUnacknowledgedMessages;
import com.turtlequeue.sdk.api.proto.Tq.CommandCloseConsumer;
import com.turtlequeue.sdk.api.proto.Tq.CommandSeek;

import com.turtlequeue.Consumer;
import com.turtlequeue.ConsumerImpl;
import com.turtlequeue.ClientImpl;
import com.turtlequeue.SubType;
import com.turtlequeue.MessageId;
import com.turtlequeue.Topic;
import com.turtlequeue.MessageIdImpl;
import com.turtlequeue.ConsumerPossibleStates;
import com.turtlequeue.StateMachine;
import com.turtlequeue.EndOfTopicMessageListener;
import com.turtlequeue.AcknowledgeBuilder.AckType;
import com.turtlequeue.GrowableArrayBlockingQueue;
import com.turtlequeue.ConsumerParams;


public class ConsumerImpl<T> implements Consumer<T> {

  private static final Logger logger = Logger.getLogger(TurtleQueueGrpc.class.getName());

  // from builder
  ClientImpl c = null;

  // TODO make these final
  // could save the conf instead
  protected Long consumerId = null;
  protected Topic topic = null;
  protected String subName = null;
  protected String consumerName = null;
  protected Integer priority = null;
  protected MessageId initialPosition = null;
  protected EndOfTopicMessageListener<T> endOfTopicMessageListener = null;

  protected Long ackTimeoutCount = null;
  protected TimeUnit ackTimeoutUnit = null;

  // internal bookkeeping
  protected ConsumerParams conf;

  //final BlockingQueue<Message<T>> incomingMessages;
  final BlockingQueue<CommandMessage> incomingMessages;
  protected Integer receiverQueueRefillThreshold = null;
  protected Integer maxReceiverQueueSize = null;
  protected Boolean hasReachedEndOfTopic = null;
  protected final ConcurrentLinkedQueue<CompletableFuture<Message<T>>> pendingReceives;
  private StateMachine<ConsumerPossibleStates> stateMachine = null;

  // Flow: number of messages that have been delivered to the application.
  // This number will be sent to the broker to notify that we are ready to get more messages
  @SuppressWarnings("rawtypes")
  private static final AtomicIntegerFieldUpdater<ConsumerImpl> AVAILABLE_PERMITS_UPDATER = AtomicIntegerFieldUpdater
    .newUpdater(ConsumerImpl.class, "availablePermits");
  @SuppressWarnings("unused")
  private volatile int availablePermits = 0;

  // initialization
  // returned to the SDK user on creation
  // allows tracking success internally
  private CompletableFuture<ConsumerImpl> subscribeReturnF = null;

  ConsumerImpl(ClientImpl client, ConsumerParams conf) {
    // copy values only to avoid user messing around?
    // also allows setting defaults
    this.c = client;
    this.conf = conf;
    this.consumerId = conf.getConsumerId();
    this.topic = conf.getTopic();
    this.subName = conf.getSubName();
    this.consumerName = conf.getConsumerName();
    this.priority = conf.getPriority();
    this.initialPosition = conf.getInitialPosition();
    this.ackTimeoutCount = conf.getAckTimeout();
    this.ackTimeoutUnit = conf.getAckTimeoutTimeUnit();
    this.endOfTopicMessageListener = conf.getEndOfTopicMessageListener();

    // Pulsar has a queue of CompletableFuture
    // that gets poured into a receiver queue

    this.maxReceiverQueueSize = conf.getReceiverQueueSize();
    this.receiverQueueRefillThreshold = conf.getReceiverQueueSize() / 2;
    this.incomingMessages = new GrowableArrayBlockingQueue<>();
    this.pendingReceives = Queues.newConcurrentLinkedQueue();
    this.hasReachedEndOfTopic = false;
    this.stateMachine = new StateMachine<ConsumerPossibleStates>().setState(ConsumerPossibleStates.Idle); // Idle

    this.c.registerConsumer(this);

    this.subscribeReturnF = new CompletableFuture<ConsumerImpl>();

    // wait until the broker acknowledges before returning the consumer
    // NOTE: could do it by allowing pre-filling the internal consumer queue (?)
    // could have (minimal)
    // TODO use return of this
    this.c.registerConsumerBroker(this).thenRun(() -> {
        logger.log(Level.FINE, "[{0}] Registering consumer success", conf);

        this.stateMachine.setState(ConsumerPossibleStates.Ready);
        subscribeReturnF.complete(this);
      }).exceptionally((ex) -> {
          logger.log(Level.WARNING, "[{0}]Registering consumer broker failed {1}", new Object[]{conf, ex});
          this.stateMachine.setState(ConsumerPossibleStates.Stopping);
          subscribeReturnF.completeExceptionally(ex);
          return null;
        });

  }


  protected void disconnect() {
    this.clearReceiverQueue();

    // could send message closing the consumer to the broker?

    this.c.consumerCommand(CommandConsumer.newBuilder()
                           .setConsumerId(this.getConsumerId())
                           .setCommandCloseConsumer(CommandCloseConsumer.newBuilder()
                                                    .build())
                           .build())
      .thenRun(() -> {
          this.stateMachine.setState(ConsumerPossibleStates.Idle);
        })
      .exceptionally(t -> {
          // expected since the conn might have been lost
          // so ignore
          this.stateMachine.setState(ConsumerPossibleStates.Idle);
          return null;
        });
  }

  protected void reconnect() {

    this.clearReceiverQueue();

    this.c.registerConsumerBroker(this).thenRun(() -> {
        logger.log(Level.FINE, "[{0}] Re-registering consumer success", conf);
        this.stateMachine.setState(ConsumerPossibleStates.Ready);
      }).exceptionally((ex) -> {
          logger.log(Level.WARNING, "[{0}]Re-registering consumer with the broker failed {1}", new Object[]{conf, ex});
          this.stateMachine.setState(ConsumerPossibleStates.Stopping);
          return null;
        });
  }


  protected void setTopicTerminated() {
    this.hasReachedEndOfTopic = true;
    if(this.endOfTopicMessageListener != null) {
      this.endOfTopicMessageListener.reachedEndOfTopic(this);
    }
  }

  public Boolean hasReachedEndOfTopic() {
    return this.hasReachedEndOfTopic;
  }



  public CompletableFuture<Boolean> hasMessageAvailable() {
    // TODO see if necessary
    // https://github.com/apache/pulsar/blob/9d44c44f01a4b753aafe73ffe67448e4c281a9f6/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java#L1979
    //
    return new CompletableFuture<Boolean>();
  }

  // see https://github.com/apache/pulsar/blob/9d44c44f01a4b753aafe73ffe67448e4c281a9f6/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java#L1911
  public CompletableFuture<Void> seek(MessageId messageId) {
    //keep impl in sync with seek below
    CompletableFuture<Void> res = new CompletableFuture<Void>();

    if((this.c.tqClient == null) || (this.c.tqClient.getState() != ConnectivityState.READY)) {
      // TODO isRetriable
      res.completeExceptionally(new TqClientException.AlreadyClosedException("Cannot seek when the TqClient is not ready"));
    } else {
      // - send message to broker
      // - wait for confirmation
      // - resume processing messages
      // - block the whole time so readNext doesn't get weird messages

      this.stateMachine.setState(ConsumerPossibleStates.Seeking);
      this.clearReceiverQueue();
      this.c.<Tq.ReplySuccess>consumerCommand(CommandConsumer.newBuilder()
                                              .setConsumerId(this.getConsumerId())
                                              .setCommandSeek(CommandSeek.newBuilder()
                                                              .setMessageId(MessageIdImpl.toMessageIdData(messageId))
                                                              .build())
                                              .build())
        .handle((Tq.ReplySuccess resp, Throwable t) -> {

            this.stateMachine.setState(ConsumerPossibleStates.Ready);

            if(t == null) {
              res.complete(null);
            } else {
              logger.log(Level.WARNING, "Error seeking consumer={0} messageId={1}", new Object[]{this, messageId});
              // propagate TqClientException
              res.completeExceptionally(t);
            }
            return null;
          });
    }
    return res;
  }

  public boolean isConnected() {
    return (this.c.tqClient != null) && (this.c.tqClient.getState() == ConnectivityState.READY);
  }

  public CompletableFuture<Void> seek(long timestamp) {
    // keep impl in sync with seek above
    CompletableFuture<Void> res = new CompletableFuture<Void>();

    if((this.c.tqClient == null) || (this.c.tqClient.getState() != ConnectivityState.READY)) {
      // TODO isRetriable
      res.completeExceptionally(new TqClientException.AlreadyClosedException("Cannot seek when the TqClient is not ready"));
    } else {

      this.stateMachine.setState(ConsumerPossibleStates.Seeking);
      this.clearReceiverQueue();

      this.c.<Tq.ReplySuccess>consumerCommand(CommandConsumer.newBuilder()
                                              .setConsumerId(this.getConsumerId())
                                              .setCommandSeek(CommandSeek.newBuilder()
                                                              .setMessagePublishTime(timestamp)
                                                              .build())
                                              .build())
        .handle((Tq.ReplySuccess resp, Throwable t) -> {

            this.stateMachine.setState(ConsumerPossibleStates.Ready);

            if(t == null) {
              res.complete(null);
            } else {
              logger.log(Level.WARNING, "Error seeking consumer={0} timestamp={1}", new Object[]{this, timestamp});
              // propagate TqClientException
              res.completeExceptionally(t);
            }
            return null;
          });
    }
    return res;
  }

  public Topic getTopic () {
    return this.topic;
  }

  protected ConsumerParams getConf() {
    return this.conf;
  }

  protected Long getConsumerId() {
    return this.consumerId;
  }

  protected void clearReceiverQueue() {
    // https://github.com/apache/pulsar/blob/870a637b4906862a611e418341dd926e21458f08/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java#L856
    //List<CommandMessage> currentMessageQueue = new ArrayList<>(this.incomingMessages.size());
    //incomingMessages.drainTo(currentMessageQueue);
    incomingMessages.clear();
    AVAILABLE_PERMITS_UPDATER.set(this, 0); // is there a different count than 0 to
                                            // keep with regards to the broker flow?
    // no need to nack - done on broker
  }


  private CompletableFuture<Message<T>> pollPendingReceive() {
    // some callbacks may have expired already, ex. .get(timeout)
    CompletableFuture<Message<T>> receivedFuture;
    while (true) {
      receivedFuture = pendingReceives.poll();
      // skip done futures (cancelling a future could mark it done)
      if (receivedFuture == null || !receivedFuture.isDone()) {
        break;
      }
    }
    return receivedFuture;
  }

  protected void enqueue(CommandMessage msg) {
    // from Client to Consumer
    // put message into internal queue
    // TODO look at
    // https://github.com/apache/pulsar/blob/983266d480f77543a29a74ac1970280abd9f804b/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerBase.java#L615-L622
    // TODO what if pending?
    // https://github.com/apache/pulsar/blob/870a637b4906862a611e418341dd926e21458f08/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java#L1251-L1284
    //
    //
    ConsumerPossibleStates state = this.stateMachine.getInternalState();
    // see duringSeek
    // https://github.com/apache/pulsar/blob/870a637b4906862a611e418341dd926e21458f08/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java#L248
    if(state != ConsumerPossibleStates.Ready) {
      // while seeking and receiveing messages: these are stragglers from the previous subscription
      // and can be discarded. Assume no need to nack as the pulsar consumer is seeking
      logger.log(Level.FINE, "[.enqueue] skipping consumerId={0} state={1} messages={2}", new Object[] {consumerId, state, msg});
      return;
    }

    if(pendingReceives.isEmpty()) {
      logger.log(Level.FINE, "[.enqueue] putting {0} in incomingMessages {1}", new Object[] {msg, incomingMessages});

      this.incomingMessages.add(msg);
    } else {
      // there are already .receive futures waiting
      logger.log(Level.FINE, "[.enqueue] there are already {0} .receive futures waiting", pendingReceives.size());
      final CompletableFuture<Message<T>> userReceiveFuture = pollPendingReceive();
      if (userReceiveFuture == null) {
        // did not find a suitable callback
        this.incomingMessages.add(msg);
        return;
      }
      userReceiveFuture.complete(messageProcessed(msg));
    }

    // TODO for batch
    // INCOMING_MESSAGES_SIZE_UPDATER.addAndGet(this, message.getData() == null ? 0 : message.getData().length);
  }

  protected CompletableFuture<ConsumerImpl> subscribeReturn() {
    // can be returned only once the reply from the broker is OK
    //this.subscribeReturnF = new CompletableFuture<Consumer>();
    // returned to the SDK on creation
    // TODO now - set the value of this.subscribeReturnF -> first subscribe
    //success or reply error
    return this.subscribeReturnF;
  }

  /**
   * send the flow command to have the broker start pushing messages
   */
  private CompletableFuture sendFlowPermitsToBroker(int numMessages) {
    return this.c.consumerCommand(CommandConsumer.newBuilder()
                                  .setConsumerId(this.getConsumerId())
                                  .setCommandFlow(CommandFlow.newBuilder()
                                                  .setMessagePermits(numMessages)
                                                  .build())
                                  .build());
  }

  protected void increaseAvailablePermits(int delta) {
    int available = AVAILABLE_PERMITS_UPDATER.addAndGet(this, delta);

    logger.log(Level.FINER, "{0}: +{1} permits, total= {2} messages, refill = {3}", new Object[]{this, delta, available, (available >= receiverQueueRefillThreshold)});

    if(available >= receiverQueueRefillThreshold) {
      sendFlowPermitsToBroker(available);


      IntUnaryOperator updateFn = (current) -> {
        logger.log(Level.FINER, "updating permits: was={0}, sentToBroker={1}, left={2}", new Object[]{current, available, (current - available)});
        return (current - available);
      };

      int left = AVAILABLE_PERMITS_UPDATER.updateAndGet(this, updateFn);
      logger.log(Level.FINER, "Permits now left are {0} for {1}\n ", new Object[]{left, this});
    }
  }

  protected synchronized void onMessageProcessed() {
    // housekeeping for the flow
    this.increaseAvailablePermits(1);
  }

  private Message<T> messageProcessed(CommandMessage msg){
    // Read the data from a stream
    InputStream in = new ByteArrayInputStream(msg.getPayload().toByteArray());
    Reader reader = TransitFactory.reader(TransitFactory.Format.JSON, in, this.c.getCustomReadHandlers(), this.c.getCustomReadDefaultHandler());
    T data = reader.read();

    //logger.log(Level.INFO, "processing message: {0} \n {1} ", new Object[]{msg, data});

    Message<T> result = new MessageImpl<T>(this.c,
                                           this,
                                           MessageIdImpl.fromMessageIdData(msg.getMessageId()),
                                           data,
                                           msg.getProducerName(),
                                           msg.getEventTime(),
                                           msg.getPublishTime(),
                                           TopicImpl.fromTqTopic(msg.getTopic()),
                                           msg.getKey(),
                                           msg.getPropertiesMap(),
                                           null,
                                           msg.getIsReplicated(),
                                           msg.getReplicatedFrom(),
                                           null,
                                           null);

    this.onMessageProcessed();

    return result;
  }

  public CompletableFuture<Message<T>> receive() {
    CompletableFuture<Message<T>> result = new CompletableFuture<Message<T>>();

    CommandMessage msg;
    // https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentLinkedQueue.html?is-external=true
    // migth need a lock as pulsar does - re-evaluate when batching is added
    try {
      msg = incomingMessages.poll(0, TimeUnit.MILLISECONDS);
      //logger.log(Level.INFO, "[.receive] did poll and got [{0}]", msg);
      if (msg == null) {
        logger.log(Level.FINE, "[.receive] called, no messages in receiverQueue");
        pendingReceives.add(result);
      } else {
        // futfeat: interceptors, call beforeConsume
        // https://github.com/apache/pulsar/blob/00ce7815f4c3428215abedb0608b7acfa2c35bd5/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerInterceptors.java#L48-L78
        //
        logger.log(Level.FINE, "[.receive] called, got message from receiverQueue {0}", msg);
        result.complete(messageProcessed(msg));
        return result;
      }
    } catch (InterruptedException ex) {
      logger.log(Level.FINE, "[.receive] Interrupt Exception");

      Thread.currentThread().interrupt();
      result.completeExceptionally(ex);
      // State state = getState();
      // if (state != State.Closing && state != State.Closed) {
      //   stats.incrementNumReceiveFailed();
      //   throw PulsarClientException.unwrap(e);
      // } else {
      //   return null;
      // }
    }

    return result;

  }

  public CommandMessage receive(long timeout, TimeUnit unit) throws Exception {
    return incomingMessages.poll(timeout, unit);
  }


  public AcknowledgeBuilder newAcknowledge() {
    return new AcknowledgeBuilder(this.c).setConsumer(this);
  }

  public CompletableFuture<Void> acknowledge(MessageId messageId) {
    return this.c.consumerCommand(CommandConsumer.newBuilder()
                                  .setConsumerId(this.getConsumerId())
                                  .setCommandAck(CommandAck.newBuilder()
                                                 .setAckType(CommandAck.AckType.INDIVIDUAL)
                                                 .setMessageId(MessageIdImpl.toMessageIdData(messageId))
                                                 .setNegativeAck(false)
                                                 .build())
                                  .build());
  }

  public CompletableFuture<Void> acknowledge(Message<T> message) {
    return this.acknowledge(message.getMessageId());
  }

  public CompletableFuture<Void> nonAcknowledge(MessageId messageId) {
    return this.c.consumerCommand(CommandConsumer.newBuilder()
                                  .setConsumerId(this.getConsumerId())
                                  .setCommandAck(CommandAck.newBuilder()
                                                 .setAckType(CommandAck.AckType.INDIVIDUAL)
                                                 .setMessageId(MessageIdImpl.toMessageIdData(messageId))
                                                 .setNegativeAck(true)
                                                 .build())
                                  .build());
  }

  public CompletableFuture<Void> nonAcknowledge(Message<T> message) {
    return this.nonAcknowledge(message.getMessageId());
  }

  public CompletableFuture<Void> redeliverUnacknowledgedMessages() {
    return this.c.consumerCommand(CommandConsumer.newBuilder()
                                  .setConsumerId(this.getConsumerId())
                                  .setRedeliverUnacknowledgedMessages(CommandRedeliverUnacknowledgedMessages.newBuilder()
                                                                      .build())
                                  .build());
  }

  public CompletableFuture<Void> acknowledgeCumulativeAsync(Message<T> message) {
    return this.c.consumerCommand(CommandConsumer.newBuilder()
                                  .setConsumerId(this.getConsumerId())
                                  .setCommandAck(CommandAck.newBuilder()
                                                 .setAckType(Tq.CommandAck.AckType.CUMULATIVE)
                                                 .setNegativeAck(false)
                                                 .setMessageId(MessageIdImpl.toMessageIdData(message.getMessageId()))
                                                 .build())
                                  .build())
      .thenApply(x -> {
          return null;
        });
  }

  protected CompletableFuture<Void> closeAsync(boolean informBroker) {

    this.stateMachine.setState(ConsumerPossibleStates.Stopping);

    if(informBroker == true) {
      ClientImpl clientRef = this.c;

      return this.c.consumerCommand(CommandConsumer.newBuilder()
                                    .setConsumerId(this.getConsumerId())
                                    .setCommandCloseConsumer(CommandCloseConsumer.newBuilder()
                                                             .build())
                                    .build())
        .thenRun(() -> {
            this.stateMachine.setState(ConsumerPossibleStates.Idle);
            clientRef.removeConsumer(this);
          });

    } else {
      this.stateMachine.setState(ConsumerPossibleStates.Idle);
      this.c.removeConsumer(this);
      return CompletableFuture.supplyAsync(() -> {
          return null;
      });
    }
  }

  @Override
  public void close() {
    this.closeAsync(true).join();
  }

  // TODO hashcode?

  @Override
  public String toString() {
    // https://github.com/apache/pulsar/blob/d7f65451dadc573fc2bb75dbb03cce705ed04d0a/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerBase.java#L614-L620
    return MoreObjects.toStringHelper(this)
      .add("consumerId", consumerId)
      .add("subscription", subName)       // subName - important
      .add("consumerName", consumerName)  // optional debugging, stats
      .add("topic", topic)
      .add("subType", conf.getSubType())
      .add("priority", priority)
      .add("receiverQueueSize", maxReceiverQueueSize)
      .add("ackTimeout", ackTimeoutCount)
      .add("ackTimeoutUnit", ackTimeoutUnit)
      .toString();
  }
}
