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
import java.util.UUID;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Properties;
import java.util.function.Function;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.io.Closeable;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

import io.grpc.stub.ClientCallStreamObserver;

import com.cognitect.transit.TransitFactory;
import com.cognitect.transit.Reader;
import com.cognitect.transit.Writer;
import com.cognitect.transit.WriteHandler;
import com.cognitect.transit.ReadHandler;
import com.cognitect.transit.DefaultReadHandler;
import com.cognitect.transit.ArrayReader;
import com.cognitect.transit.MapReader;
import com.cognitect.transit.Reader;

import com.google.common.base.MoreObjects;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel ;

import com.turtlequeue.sdk.api.proto.Tq;
import com.turtlequeue.sdk.api.proto.TurtleQueueGrpc;
import com.turtlequeue.sdk.api.proto.Tq.CommandConnect;
import com.turtlequeue.sdk.api.proto.Tq.CommandPublish;
import com.turtlequeue.sdk.api.proto.Tq.CommandSubscribe;
import com.turtlequeue.sdk.api.proto.Tq.ResponsePublish;
import com.turtlequeue.sdk.api.proto.Tq.ClientToBroker;
import com.turtlequeue.sdk.api.proto.Tq.CommandPing;
import com.turtlequeue.sdk.api.proto.Tq.BrokerToClient;
import com.turtlequeue.sdk.api.proto.Tq.ReplyConnect;
import com.turtlequeue.sdk.api.proto.Tq.ReplyPong;
import com.turtlequeue.sdk.api.proto.Tq.CommandMessage;
import com.turtlequeue.sdk.api.proto.Tq.ReplySuccess;
import com.turtlequeue.sdk.api.proto.Tq.CommandConsumer;
import com.turtlequeue.sdk.api.proto.Tq.CommandEndOfTopic;
import com.turtlequeue.sdk.api.proto.Tq.CommandProducer;
import com.turtlequeue.sdk.api.proto.Tq.CommandProducerCreate;

import com.turtlequeue.sdk.api.proto.TurtleQueueGrpc.TurtleQueueStub;
import com.turtlequeue.sdk.api.proto.Tq.BrokerToClient.BtocOneofCase;

import com.turtlequeue.ClientBuilder;
import com.turtlequeue.ClientImpl;
import com.turtlequeue.MessageId;
import com.turtlequeue.MessageIdImpl;
import com.turtlequeue.TqClientException;
import com.turtlequeue.Consumer;
import com.turtlequeue.ConsumerParams;
import com.turtlequeue.AcknowledgeBuilder.AckType;
import com.turtlequeue.Admin;
import com.turtlequeue.StateMachine;
import com.turtlequeue.ClientPossibleStates;
import com.turtlequeue.ConsumerBuilder;
import com.turtlequeue.SubscriptionMode;
import com.turtlequeue.Encoder;

//
// https://github.com/saturnism/grpc-by-example-java/blob/master/error-handling-example/error-handling-client/src/main/java/com/example/grpc/client/ErrorHandlingGrpcClient.java
// https://github.com/saturnism/grpc-by-example-java/blob/master/simple-grpc-client/src/main/java/com/example/grpc/client/MyGrpcClient.java#L38//
//
public class ClientImpl implements Client {

  private static final Logger logger = Logger.getLogger(TurtleQueueGrpc.class.getName());

  // builder, user params
  String host = "";
  Integer port = -1;
  Boolean secure = true;
  String userToken = null;
  String apiKey = null;

  // hardcoded
  final static Properties versionInfo = Version.getVersion();

  // internal comms

  // reused by admin
  protected TqClient tqClient = null;

  // pluggable admin
  protected static Class<Admin> adminClass = null;
  protected Admin admin = null;

  //
  // last command out, for ping
  //


  //Object state = null ; // TODO enum: grpc state + connection / auth
  //CompletableFuture<Client>() connectResponse = null; // contains server settings


  StreamObserver<Tq.ClientToBroker> clientToBroker = null;
  StreamObserver<Tq.BrokerToClient> brokerToClient = null;


  // internal, set by the server. Changes on reconnection
  UUID connectionUid = null;

  StateMachine<ClientPossibleStates> stateMachine = null;

  ScheduledExecutorService pingLoopService = null;
  long lastReplyPong = 0;
  ScheduledFuture pingPongFut = null;

  ScheduledExecutorService stateChangeWatcher = null;
  ScheduledFuture stateChangeFut = null;

  //
  // state that would need to be restored in case of reconnect:
  // subscriptions params + handlers
  //
  // List<Subscriptions>
  // List<>
  // TODO BrokerToClient?? or what?
  Map<Long, CompletableFuture> pendingRequests = null;

  private final AtomicLong requestIdGenerator;
  private final AtomicLong consumerIdGenerator;
  private final AtomicLong producerIdGenerator;

  // consumerId, Consumer (has config)
  ConcurrentHashMap<Long, ConsumerImpl> consumerRegistry = new ConcurrentHashMap<Long, ConsumerImpl>();
  ConcurrentHashMap<Long, ProducerImpl> producerRegistry = new ConcurrentHashMap<Long, ProducerImpl>();

  Map<String, ReadHandler<?, ?>> customReadHandlers = null;
  Map<Class, WriteHandler<?, ?>> customWriteHandlers = null;
  DefaultReadHandler<?> customReadDefaultHandler = null;
  WriteHandler<?, ?> customDefaultWriteHandler = null;
  MapReader<?, Map<Object, Object>, Object, Object> mapBuilder = null;
  ArrayReader<?, List<Object>, Object> listBuilder = null;
  Function<InputStream, Reader> transitReader = null;
  Function<OutputStream, Writer> transitWriter = null;

  public ClientImpl(String host, Integer port, Boolean secure, String userToken, String apiKey,
                    Function<InputStream, Reader> transitReader,
                    Function<OutputStream, Writer> transitWriter,
                    Map<String, ReadHandler<?, ?>> customReadHandlers,
                    Map<Class, WriteHandler<?, ?>> customWriteHandlers,
                    DefaultReadHandler<?> customReadDefaultHandler,
                    WriteHandler<?, ?> customDefaultWriteHandler,
                    MapReader<?, Map<Object, Object>, Object, Object> mapBuilder,
                    ArrayReader<?, List<Object>, Object> listBuilder) {
    this.host = host;
    this.port = port;
    this.secure = secure;
    this.userToken = userToken;
    this.apiKey = apiKey;

    this.customReadHandlers = customReadHandlers;
    this.customWriteHandlers = customWriteHandlers;
    this.customReadDefaultHandler = customReadDefaultHandler;
    this.customDefaultWriteHandler = customDefaultWriteHandler;
    this.mapBuilder = mapBuilder;
    this.listBuilder = listBuilder;
    this.transitReader = transitReader;
    this.transitWriter = transitWriter;

    this.pendingRequests = new ConcurrentHashMap<Long, CompletableFuture>();

    this.requestIdGenerator = new AtomicLong(1);
    this.consumerIdGenerator = new AtomicLong(1);
    this.producerIdGenerator = new AtomicLong(1);
    pingLoopService = Executors.newScheduledThreadPool(1);
    stateChangeWatcher = Executors.newScheduledThreadPool(1);

    ((ScheduledThreadPoolExecutor) pingLoopService).setRemoveOnCancelPolicy(true);

    this.stateMachine = new StateMachine<ClientPossibleStates>().setState(ClientPossibleStates.Idle);
    tqClient = new TqClient(this);
  }

  public String getHost() {
    return this.host;
  }

  public int getPort() {
    return this.port;
  }

  public Boolean getSecure() {
    if(this.secure == null) {
      return true; // default
    } else {
      return this.secure;
    }
  }

  protected String getAuthMethod() {
    // TODO similar to <type>
    // https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#Authentication_schemes
    // TODO Bearer
    return "Token" ;// TODO this.conf.getAuthMethod();
  }

  protected String getDataFormat() {
    // like an http content-type header
    return "application/transit+json";
  }

  protected Map<String, ReadHandler<?, ?>> getCustomReadHandlers() {
    return this.customReadHandlers;
  }

  protected Map<Class, WriteHandler<?, ?>>  getCustomWriteHandlers() {
    return this.customWriteHandlers;
  }

  protected DefaultReadHandler<?> getCustomReadDefaultHandler() {
    return this.customReadDefaultHandler;
  }

  protected WriteHandler<?, ?> getCustomDefaultWriteHandler() {
    return this.customDefaultWriteHandler;
  }

  protected MapReader<?, Map<Object, Object>, Object, Object> getMapBuilder() {
    return this.mapBuilder;
  }

  protected ArrayReader<?, List<Object>, Object> getListBuilder() {
    return this.listBuilder;
  }

  protected Function<InputStream, Reader> getTransitReader() {
    return this.transitReader;
  }

  protected Function<OutputStream, Writer> getTransitWriter() {
    return this.transitWriter;
  }

  protected String getSdkVersion() {
    return "java:" + this.getVersionInfo().getProperty("groupId") + ":" +
      this.getVersionInfo().getProperty("artifactId") + ":" +
      this.getVersionInfo().getProperty("version");
  }

  private class WatchStateCallable implements Runnable {
    private final ClientImpl client;

    WatchStateCallable(ClientImpl client) {
      this.client = client;
    }

    @Override
    public void run()  {
      try {
        client.checkIfReconnected();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException ex) {
        // aborted
      } catch (TimeoutException ex) {
        // took too long
      }
    }
  }


  private class PingCallable implements Runnable {
    private final ClientImpl client;

    PingCallable(ClientImpl client) {
      this.client = client;
    }

    @Override
    public void run()  {
      try {
        client.pingPong();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException ex) {
        // aborted
      } catch (TimeoutException ex) {
        // took too long
      }
    }
  }

  private <T> CompletableFuture<T> timeoutAfter(long timeout, TimeUnit unit) {
    CompletableFuture<T> result = new CompletableFuture<T>();
    this.pingLoopService.schedule(() -> result.completeExceptionally(new TimeoutException()), timeout, unit);
    return result;
  }

  private void reconnectConsumers() {
    Iterator<Map.Entry<Long, ConsumerImpl>> it = consumerRegistry.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Long, ConsumerImpl> pair = it.next();
      ConsumerImpl c = pair.getValue();
      c.reconnect();
    }
  }

  private void reconnectProducers() {
    Iterator<Map.Entry<Long, ProducerImpl>> it = producerRegistry.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Long, ProducerImpl> pair = it.next();
      ProducerImpl p = pair.getValue();
      p.reconnect();
    }
  }

  private void checkIfReconnected()  throws InterruptedException, ExecutionException, TimeoutException  {
    // ((ManagedChannel) clientRef.tqClient.getChannel()).notifyWhenStateChanged(ConnectivityState.IDLE, () -> {
    //     ConnectivityState st = clientRef.tqClient.getState();
    //     logger.log(Level.WARNING , "Channel state transition: from IDLE to " + st);
    //     if(st == ConnectivityState.READY) {
    //       this.reconnectConsumers();
    //     } else {
    //       // still need to loop
    //       // PROBLEM - looping here will only add another loop..
    //       stateChangeFut = stateChangeWatcher.scheduleWithFixedDelay(new WatchStateCallable(clientRef), 0, 100, TimeUnit.MILLISECONDS);

    //       waitForReady();
    //     }
    //   });
    ConnectivityState st = this.tqClient.getState();
    if(st == ConnectivityState.READY) {
      try {
        // recreates the stub
        // apparently this is needed :/
        this.connect().get(2, TimeUnit.SECONDS);
      } catch (Exception ex) {
        logger.log(Level.INFO, "Failed to reconnect {0}", ex);
      }
      // stop looping
      stateChangeFut.cancel(false);
    }
  }

  private void pingPong() throws InterruptedException, ExecutionException, TimeoutException {
    //
    // TODO healthcheck
    // https://github.com/grpc/grpc/blob/master/doc/health-checking.md
    // instead would allow using the health component
    //
    ConnectivityState st = this.tqClient.getState();
    if(st == ConnectivityState.READY) {
      long requestId = this.getNextRequestId();
      this.clientToBrokerOnNext(ClientToBroker.newBuilder()
                                .setRequestId(requestId)
                                .setCommandPing(CommandPing.newBuilder().build())
                                .build());

      this.<ReplyPong>waitForResponse(requestId)
        .acceptEither(timeoutAfter(15, TimeUnit.SECONDS),
                      (replyPong) -> {
                        this.pendingRequests.remove(requestId);
                      })
        .exceptionally((ex) -> {
            logger.log(Level.WARNING, "Ping timeout requestId={0}, State={1}, Exception:\n{2}", new Object[]{requestId, st, ex});
            this.pendingRequests.remove(requestId);
            return null;
          });
    } else {
      logger.log(Level.WARNING, "Cannot ping, the connections is State={0}", new Object[]{st});
    }
  }

  protected void stopPingLoop() {

  }

  protected void registerConsumer(ConsumerImpl consumer) {
    logger.log(Level.FINE , "Registering new consumer {0}", consumer);
    this.consumerRegistry.put(consumer.getConsumerId(), consumer);
  }

  protected void registerProducer(ProducerImpl producer) {
    logger.log(Level.FINE , "Registering new producer {0}", producer);
    this.producerRegistry.put(producer.getProducerId(), producer);
  }

  protected void removeConsumer(ConsumerImpl consumer) {
    logger.log(Level.FINE , "Removing consumer {0}", consumer);
    this.consumerRegistry.remove(consumer.getConsumerId(), consumer);
  }

  protected void removeProducer(ProducerImpl producer) {
    logger.log(Level.FINE , "Removing producer {0}", producer);
    this.producerRegistry.remove(producer.getProducerId(), producer);
  }


  public CompletableFuture<Void> registerConsumerBroker(ConsumerImpl consumer) {

    ConsumerParams conf = consumer.getConf();
    logger.log(Level.FINE , "Registering new consumer " + consumer.getConsumerId() + " with broker " + conf);

    CommandSubscribe.Builder cmdBuilder = CommandSubscribe.newBuilder()
      .setConsumerId(consumer.getConsumerId()) // FIXME check logic + leave note here
      .setTopic(TopicImpl.toTqTopic(conf.getTopic()))
      .setSubName(conf.getSubName())
      .setSubType(conf.getSubType().toTqSubType())
      .setPriorityLevel(conf.getPriority())
      .setStartMessageId(MessageIdImpl.toMessageIdData(conf.getInitialPosition()))
      .setReceiverQueueSize(conf.getReceiverQueueSize())
      .putAllMeta(conf.getMetadata());

    if (conf.getAckTimeout() != null && conf.getAckTimeoutTimeUnit() != null) {
      cmdBuilder.setAckTimeoutUnit(Encoder.javaTimeUnitToProtobuf(conf.getAckTimeoutTimeUnit()));
      cmdBuilder.setAckTimeoutValue(conf.getAckTimeout());
    }

    if(conf.getConsumerName() != null) { // optional
      cmdBuilder.setConsumerName(conf.getConsumerName());
    }

    if(conf.getSubscriptionMode() != SubscriptionMode.Durable) { // default
      cmdBuilder.setSubscriptionMode(conf.getSubscriptionMode().toTqSubscriptionMode());
    }

    if(conf.getJsonPath() != null) { // reader
      cmdBuilder.setJsonPath(conf.getJsonPath());
    }

    CommandSubscribe cmd = cmdBuilder.build();
    long requestId = this.getNextRequestId();
    this.clientToBrokerOnNext(ClientToBroker.newBuilder()
                               .setRequestId(requestId)
                               .setCommandSubscribe(cmd)
                               .build());

    return this.<Void>waitForResponse(requestId);
  }

public CompletableFuture<Void> registerProducerBroker(ProducerImpl producer) {

    ProducerParams conf = producer.getConf();
    logger.log(Level.FINE, "Registering new producer " + producer.getProducerId() + " with broker " + conf);

    CommandProducerCreate.Builder cmdBuilder = CommandProducerCreate.newBuilder()
      .setProducerId(producer.getProducerId())
      .setTopic(TopicImpl.toTqTopic(conf.getTopic()))
      // TODO
      //.setProperties(conf.getProperties())
      //.setEncryptionKey(conf.getEncryptionKey())
      //.putAllMeta(conf.getMetadata())
      ;

    if(conf.getProducerName() != null) {
      cmdBuilder.setProducerName(conf.getProducerName());
    }

    if (conf.getEnableBatching() != null) {
      cmdBuilder.setEnableBatching(conf.getEnableBatching());
      // TODO
      // if(conf.batchingMaxMessages() != null) {
      //   cmdBuilder.setBatchingMaxMessages(conf.batchinMaxMessages());
      // }
    }

    // TODO
    // if (conf.getBatchinMaxPublishDelayUnit() != null && conf.getBatchinMaxPublishDelayValue() != null) {
    //   cmdBuilder.setAckTimeoutUnit(Encoder.javaTimeUnitToProtobuf(conf.getBatchinMaxPublishDelayUnit()));
    //   cmdBuilder.setAckTimeoutValue(conf.getBatchinMaxPublishDelayValue());
    // }

    // if (conf.getMessageRoutingMode() != null ) {
    //   // TODO
    // }

    // if (conf.getCompressionType() != null) {
    //   // TODO
    // }

    if(conf.getMaxPendingMessages() != null) {
      cmdBuilder.setMaxPendingMessages(conf.getMaxPendingMessages());
    }

    if (conf.getSendTimeoutUnit() != null && conf.getSendTimeoutValue() != null) {
      cmdBuilder.setSendTimeoutUnit(Encoder.javaTimeUnitToProtobuf(conf.getSendTimeoutUnit()));
      cmdBuilder.setSendTimeoutValue(conf.getSendTimeoutValue());
    }

    if(conf.getHashingScheme() != null) {
      cmdBuilder.setHashingScheme(conf.getHashingScheme().toTqHashingScheme());
    }


    if(conf.getBlockIfQueueFull() != null) {
      cmdBuilder.setBlockIfQueueFull(conf.getBlockIfQueueFull());
    }

    CommandProducerCreate cmd = cmdBuilder.build();
    long requestId = this.getNextRequestId();
    this.clientToBrokerOnNext(ClientToBroker.newBuilder()
                              .setRequestId(requestId)
                              .setCommandProducerCreate(cmd)
                              .build());

    return this.<Void>waitForResponse(requestId);
  }


  public Properties getVersionInfo () {
    return this.versionInfo;
  }

  protected void checkState(String desc) {
    this.stateMachine.checkStateIs(ClientPossibleStates.Ready, desc);
  }

  public CompletableFuture<Client> connect () throws Exception {

    TurtleQueueStub stub = this.tqClient.checkAndGetStub(true);

    final ClientImpl clientRef = this;
    CompletableFuture<Client> connectResponse = new CompletableFuture<Client>();

    // Deadline exceeded
    // Server-side can listen to Cancellations

    // setup incoming messages
    this.brokerToClient = new StreamObserver<Tq.BrokerToClient>() {
        @Override
        public void onNext(BrokerToClient c) {
          try {
            Long requestId = c.getRequestId();

            logger.log(Level.FINE, "Broker says: " + requestId + "  " + c.getBtocOneofCase() + "\n" + c);

            switch (c.getBtocOneofCase()) {

            case COMMAND_PING:
              // TODO store in state last ping date
              clientRef.clientToBrokerOnNext(ClientToBroker.newBuilder()
                                             .setReplyPong(ReplyPong.newBuilder().build())
                                             .build());
              break;

            case REPLY_CONNECT:
              // server says we're connected, return the promise
              // and start pinging
              {
                UUID newUuid = UUID.fromString(c.getReplyConnect().getUuid());

                if ((clientRef.connectionUid != null) && (clientRef.connectionUid != newUuid)) {
                  logger.log(Level.FINE, "Broker replied, reconnection complete {0}", newUuid);

                  // On the other case "UNAVAILABLE", the Streams have to be
                  // re-created, and the consumers discarded + re-created
                  // (maybe, would be neat to avoid them knowing about streams)
                  // https://groups.google.com/g/grpc-io/c/PEFwhLXT2wo
                  //
                  //
                  // TODO could reset requestIds etc.
                  //
                  clientRef.connectionUid = newUuid;
                  clientRef.reconnectConsumers();
                } else {
                  // first connection
                  logger.log(Level.FINE, "Broker replied, handshake complete {0}", newUuid);
                  clientRef.connectionUid = newUuid;
                  pingPongFut = pingLoopService.scheduleWithFixedDelay(new PingCallable(clientRef), 10, 20, TimeUnit.SECONDS);
                  connectResponse.complete(clientRef);
                }
              }
              break;

            case REPLY_PONG:
              //
              // client-driven pong response: store last response from server
              // this should help when the server is re-deployed
              {
                ReplyPong success = c.getReplyPong();
                clientRef.deliverResponse(requestId, success);
              }
              break;

            case COMMAND_MESSAGE:
              // dispatch to consumer
              {
                CommandMessage msg = c.getCommandMessage();
                Long consumerId = msg.getConsumerId();
                ConsumerImpl consumer  = clientRef.consumerRegistry.get(consumerId);
                if (consumer != null) {
                  consumer.enqueue(c.getCommandMessage());
                } else {
                  logger.log(Level.INFO, "Received message for consumer already closed. {requestId=" + requestId + " consumerId=" + consumerId + "}");
                  logger.log(Level.FINE, "Consumers present are: " + clientRef.consumerRegistry);

                  // send close to broker just in case?
                }
              }
              break;

            case COMMAND_END_OF_TOPIC:
              {
                CommandEndOfTopic eot = c.getCommandEndOfTopic();
                Long consumerId = eot.getConsumerId();
                ConsumerImpl consumer  = clientRef.consumerRegistry.get(consumerId);
                if (consumer != null) {
                  consumer.setTopicTerminated();
                } else {
                  logger.log(Level.INFO, "Received endOfTopic for consumer already closed" + requestId + "consumerId=" + consumerId);
                }
              }
              break;

            case RESPONSE_PUBLISH:
              {
                // message published to broker, contains MessageId
                ResponsePublish rp = c.getResponsePublish();
                clientRef.deliverResponse(requestId, rp);
              }
              break;

            case REPLY_SUCCESS:
              // generic reply success, used to
              // signal that the consumer creation on the broker has been done
              // successfully ( see registerConsumerBroker)
              //
              {
                ReplySuccess success = c.getReplySuccess();
                clientRef.deliverResponse(requestId, success);
              }
              break;

            case REPLY_ERROR:
              {
                logger.log(Level.SEVERE, "Error from the broker " + requestId);

                TqClientException ex = TqClientException.makeTqExceptionFromReplyError(c.getReplyError());
                logger.log(Level.SEVERE, "Error from the broker as exception " + ex);

                if(requestId != 0 ) {
                  clientRef.pendingRequests.get(requestId).completeExceptionally(ex);
                  clientRef.pendingRequests.remove(requestId);
                } else {
                  logger.log(Level.WARNING, "Could not find matching request for error" + requestId + ex);
                }
              }
              break;

            case BTOCONEOF_NOT_SET:
              logger.log(Level.WARNING, "Unknown message received {0}", c);
              break;

            default:
              logger.log(Level.WARNING, "Unimplemented message received {0}", c);
            }
          } catch (Exception ex) {
            logger.log(Level.SEVERE, "Unhandled SDK exception message={0}", c);
            logger.log(Level.SEVERE, "Unhandled SDK exception error={0}", ex);
          }
        }

        @Override
        public void onError(Throwable t) {
          // https://github.com/hyperledger/fabric-sdk-java/blob/master/src/main/java/org/hyperledger/fabric/sdk/OrdererClient.java#L194
          // the broker no longer wishes to talk to us
          // https://grpc.github.io/grpc-java/javadoc/io/grpc/ClientInterceptor.html
          // TODO see above for auth on reconnect... BUT would need to also handle re-subs too..
          // and also add logging interceptor

          // broker is sending an error back.. get the metadata requestId
          // TODO check requestId here and handle below correctly, may not be for this request..
          logger.log(Level.FINE , "onError called {0} ", t);
          Status st = Status.fromThrowable(t);

          // System.out.println("CHECK: CONNECTION STATUS " + clientRef.tqClient.getState());

          // Status st = Status.fromThrowable(t);
          // st.getCode();

          if (!connectResponse.isDone()) { // race condition?
            logger.log(Level.SEVERE , "Cannot connect, please check credentials and connectivity {0} " + connectResponse, t);
            connectResponse.completeExceptionally(t);
          } else if (clientRef.getConnState() == ConnectivityState.SHUTDOWN) {
            // https://github.com/grpc/grpc-java/issues/3297#issuecomment-346427628
            // however this could be caused by another error so still log it
            //
            logger.log(Level.FINE , "onError while shutdown - ignoring " + st);
          } else {
            // if onError State=READY BrokerReplyStatus=Status{code=CANCELLED, description=Client is closing, cause=null}
            // then this is fine
            logger.log(Level.FINE , "onError State={0} BrokerReplyStatus={1}", new Object[]{clientRef.tqClient.getState(), st});

            if((clientRef.tqClient.getState() == ConnectivityState.IDLE)
               && ((st.getCode() == Status.Code.UNAVAILABLE)
                   || (st.getCode() == Status.Code.CANCELLED))) {
              // the server cancelled us and we want to stay connected
              logger.log(Level.INFO , "Detected disconnection, initiating reconnect");

              //stateChangeFut = stateChangeWatcher.scheduleWithFixedDelay(new WatchStateCallable(clientRef), 0, 100, TimeUnit.MILLISECONDS);
              // clientRef.tqClient.reconnectChannel();
              // OR
              try {
                // recreates the stub
                // apparently this is needed :/
                clientRef.connect();
              } catch (Exception ex) {
                logger.log(Level.SEVERE, "Failed to initiate reconnect {0}", ex);
              }
              //
              // TODO could use notifyWhenStateChanged ?
              // ((ManagedChannel) clientRef.tqClient.getChannel()).notifyWhenStateChanged(ConnectivityState.IDLE, () -> {
              //     // assume the state change is that the client is now
              //     // connected. Only log it but don't use getState
              //     ConnectivityState st = clientRef.tqClient.getState();
              //     logger.log(Level.WARNING , "Channel state transition: from IDLE to " + st);
              //     if(st == ConnectivityState.READY) {
              //       this.reconnectConsumers();
              //     } else {
              //       waitForReady();
              //     }
              //   });
            }

            // TODO see if there can be a requestId here?
            // TODO try to find a callback to throw? or a user .onError?

          }
        }

        @Override
        public void onCompleted() {
          System.out.println("Broker has closed bidi link TODO reconnect depending on state ?");
        }
      };

    try {
      this.clientToBroker = stub.bidilink(this.brokerToClient);

      if(this.userToken == null) {
        logger.log(Level.SEVERE, "Missing user token");
      }
      if(this.apiKey == null) {
        logger.log(Level.SEVERE, "Missing api key");
      }

    } catch (StatusRuntimeException e) {
      logger.log(Level.INFO, "RPC failed: {0}", e.getStatus());
      throw e;
    }

    return connectResponse;
  }

  protected void clientToBrokerOnNext(ClientToBroker cmd) {
    // https://github.com/grpc/grpc-java/issues/5997
    // Since individual StreamObservers are not thread-safe, if multiple threads will be writing to a StreamObserver concurrently, the application must synchronize calls.
    // logger.log(Level.INFO , "clientToBrokerOnNext: {0}", cmd);
    synchronized (this.clientToBroker) {
      this.clientToBroker.onNext(cmd);
    }
  }

  // Client interface?
  public ConnectivityState getConnState () {
    return this.tqClient.getState();
  }

  // public ConsumerBuilder newConsumer {
  //   return new ConsumerBuilder();
  // }

  /**
     puts a promise in the registry, and returns it.
     We MUST know the expected return type at this point
  */
  private <T> CompletableFuture<T> waitForResponse(Long requestId) {
    CompletableFuture<T> res =  new CompletableFuture<T>();
    //
    //
    // TODO https://github.com/apache/pulsar/blob/054f18b35c3da81878f331689d1a85e29adbf592/pulsar-broker/src/main/java/org/apache/pulsar/broker/transaction/buffer/impl/TransactionBufferHandlerImpl.java#L236-L238
    //
    this.pendingRequests.put(requestId, res);
    return res;
  }

  private <T> void deliverResponse(Long requestId, T data) {
    try {
      //logger.log(Level.INFO , "Delivering response " + requestId + " --- " + data);

      CompletableFuture<T> f = this.pendingRequests.get(requestId);
      if (f != null) {
        logger.log(Level.FINE, "Delivering response to " + f);
        f.complete(data);
        this.pendingRequests.remove(requestId);
      } else {
        logger.log(Level.WARNING , "Could not deliver response, missing requestId " + requestId);
      }

    } catch (Exception ex) {
      logger.log(Level.SEVERE , "Could not deliver response, exception ", ex);
    }
  }

  //
  // or arguments?
  // https://stackoverflow.com/questions/9863742/how-to-pass-an-arraylist-to-a-varargs-method-parameter
  //
  //
  // private void deliverResponse(Long requestId, Object... arguments) {

  //   // method(strs.toArray(new String[strs.size()]));
  //   //this.pendingRequests.get(requestId)(arguments);
  //   // CompletableFuture<BrokerToClient> promiseResult = this.pendingRequests.get(requestId);
  //   //promiseResult.deliver(arguments.toArray());

  //   // this.pendingRequests = new ConcurrentHashMap<Long, CompletableFuture<BrokerToClient>>();
  // }


  // need one per message type ? annoying..
  private void deliverPublishResponse (Long requestId, MessageId messageId) {
    // CompletableFuture<MessageId> promiseResult = this.pendingPublishResponse.get(requestId);
    // //promiseResult.put(messageId);
    // promiseResult.complete(messageId);

    //CompletableFuture<MessageId>
    CompletableFuture<Object> promiseResult = this.pendingRequests.get(requestId);
    //promiseResult.put(messageId);
    promiseResult.complete(messageId);
  }


  // for retry
  // https://github.com/apache/pulsar/blob/cb9a3a7239bd477aae2b7e714f96f4ca28c8b1c1/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ProducerImpl.java#L1157
  //


  public long getNextRequestId() {
    return this.requestIdGenerator.getAndIncrement();
  }

  public long getNextConsumerId() {
    return this.consumerIdGenerator.getAndIncrement();
  }

  public long getNextProducerId() {
    return this.producerIdGenerator.getAndIncrement();
  }


  // moved to ProducerImpl
  // "easy" api

  // public PublishParamsBuilder publishParams() {
  //   return new PublishParamsBuilder();
  // }

  // TODO <T>
  // public CompletableFuture<MessageId> publish(PublishParams params) {

  //   // TODO check conn state?

  //   long requestId = this.getNextRequestId();

  //   CompletableFuture<MessageId> response = this.<MessageId>waitForResponse(requestId);

  //   // Write the data to a stream
  //   //OutputStream
  //   ByteString.Output out = com.google.protobuf.ByteString.newOutput(); // = new ByteArrayOutputStream();
  //   Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, out); // TODO pass config
  //   writer.write(params.getPayload());


  //   CommandPublish.Builder b = CommandPublish.newBuilder()
  //     .setTopic(TopicImpl.toTqTopic(params.getTopic()))
  //     .setPayload(out.toByteString());

  //   if(params.getProducerName() != null) { // optional
  //     b.setProducerName(params.getProducerName());
  //   }

  //   if(params.getEventTime() != null) { // optional
  //     b.setEventTime(params.getEventTime());
  //   }

  //   if(params.getKey() != null) { // optional
  //     b.setKey(params.getKey());
  //   }

  //   if(params.getReplicationDisabled() != null) { // optional
  //     b.setReplicationDisabled(params.getReplicationDisabled());
  //   }

  //   if(params.getProperties() != null) { // optional
  //     b.putAllProperties(params.getProperties());
  //   }
  //   if(params.getDelayTimeUnit() != null && params.getDelayTimeValue() != null) { // optional
  //     b.setDelayTimeUnit(javaTimeUnitToProtobuf(params.getDelayTimeUnit()));
  //     b.setDelayTimeValue(params.getDelayTimeValue());
  //   }

  //   clientToBrokerOnNext(ClientToBroker.newBuilder()
  //                        .setRequestId(requestId)
  //                        .setCommandPublish(b.build())
  //                        .build());

  //   return response;
  // };
  // FIXME replace by producerCommand
  // public CompletableFuture<MessageId> publish(ProducerImpl producer, Message message) {

  //   // TODO check conn state?

  //   long requestId = this.getNextRequestId();

  //   CompletableFuture<MessageId> response = this.<MessageId>waitForResponse(requestId);

  //   // Write the data to a stream
  //   // OutputStream
  //   ByteString.Output out = com.google.protobuf.ByteString.newOutput(); // = new ByteArrayOutputStream();
  //   Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, out); // TODO pass config
  //   writer.write(params.getPayload());

  //   CommandPublish.Builder b = CommandPublish.newBuilder()
  //     .setTopic(TopicImpl.toTqTopic(params.getTopic()))
  //     .setPayload(out.toByteString());

  //   if(params.getProducerName() != null) { // optional
  //     b.setProducerName(params.getProducerName());
  //   }

  //   if(params.getEventTime() != null) { // optional
  //     b.setEventTime(params.getEventTime());
  //   }

  //   if(params.getKey() != null) { // optional
  //     b.setKey(params.getKey());
  //   }

  //   if(params.getReplicationDisabled() != null) { // optional
  //     b.setReplicationDisabled(params.getReplicationDisabled());
  //   }

  //   if(params.getProperties() != null) { // optional
  //     b.putAllProperties(params.getProperties());
  //   }
  //   if(params.getDelayTimeUnit() != null && params.getDelayTimeValue() != null) { // optional
  //     b.setDelayTimeUnit(javaTimeUnitToProtobuf(params.getDelayTimeUnit()));
  //     b.setDelayTimeValue(params.getDelayTimeValue());
  //   }

  //   clientToBrokerOnNext(ClientToBroker.newBuilder()
  //                        .setRequestId(requestId)
  //                        .setCommandPublish(b.build())
  //                        .build());

  //   return response;
  // };


  public ConsumerBuilder newConsumer() {
    return new ConsumerBuilder(this);
  };
  public ProducerBuilder newProducer() {
    return new ProducerBuilder(this);
  };


  public ReaderBuilder newReader() {
    return new ReaderBuilder(this);
  };

  public AcknowledgeBuilder newAcknowledge() {
    return new AcknowledgeBuilder(this);
  }

  protected <T> CompletableFuture<T> consumerCommand(CommandConsumer commandConsumer) {
    long requestId = this.getNextRequestId();

    this.clientToBrokerOnNext(ClientToBroker.newBuilder()
                               .setRequestId(requestId)
                               .setCommandConsumer(commandConsumer)
                               .build());

    return this.<T>waitForResponse(requestId);
  }

  protected <T> CompletableFuture<T> producerCommand(CommandProducer commandProducer) {
    long requestId = this.getNextRequestId();

    this.clientToBrokerOnNext(ClientToBroker.newBuilder()
                              .setRequestId(requestId)
                              .setCommandProducer(commandProducer)
                              .build());

    return this.<T>waitForResponse(requestId);
  }

  protected static void registerAdmin(Class a) {
    adminClass = a;
  }

  public Admin admin() {
    if(this.admin != null) {
      return this.admin;
    } else if(com.turtlequeue.ClientImpl.adminClass == null) {
      logger.log(Level.SEVERE , "Admin dependency is missing. Please import com.turtlequeue.admin and initialize it before using it");
      // throw new Exception("Admin dependency is missing");
      return null;
    } else {
      try {
        //Object ob = this.adminClass.newInstance(this);

        Object ob = com.turtlequeue.ClientImpl.adminClass.getDeclaredConstructor(this.getClass()).newInstance(this);
        this.admin = (Admin) ob;

        return this.admin;
      }//  catch (InstantiationException ex) {
      //   logger.log(Level.SEVERE , "Error creating new admin. {0}", ex);
      //   throw(ex);
      // } catch (IllegalAccessException ex) {
      //   logger.log(Level.SEVERE , "Error creating new admin. {0}", ex);
      //   throw(ex);
      // }
      catch (Exception ex) {
        logger.log(Level.SEVERE , "Error creating new admin. {0}", ex);
        // throw(ex);
        return null;
      }
    }
  }

  public TopicBuilderImpl newTopicBuilder() {
    return new TopicBuilderImpl();
  }

  protected String getUserToken() {
    return this.userToken;
  }

  protected String getApiKey() {
    return this.apiKey;
  }

  public void close() throws Exception {
    // close from the ClientImpl
    // set a close marker state internally, disallow new operations
    // https://stackoverflow.com/questions/63250570/how-to-cancel-a-grpc-streaming-call
    //
    // TODO cleanup consumers too?
    // completableFuture for tqClient

    // TODO set a state marker here
    // and disallow new operations
    //
    this.stateMachine.setState(ClientPossibleStates.Stopping);

    if(pingPongFut != null) {
      pingPongFut.cancel(true);
    }

    pingLoopService.shutdownNow();

    if(stateChangeFut != null) {
      stateChangeFut.cancel(true);
    }

    stateChangeWatcher.shutdownNow();

    Iterator<Map.Entry<Long, ConsumerImpl>> it = consumerRegistry.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Long, ConsumerImpl> pair = it.next();
      // System.out.println(pair.getKey() + " = " + pair.getValue());
      ConsumerImpl c = pair.getValue();
      c.closeAsync(false);
      it.remove(); // avoids a ConcurrentModificationException
    }

    // close the stream (half-close)?
    // this is a callback
    // this.clientToBroker.onCompleted();
    // https://github.com/grpc/grpc-java/issues/3095#issuecomment-338724284
    // https://stackoverflow.com/questions/43414695/closing-all-open-streams-in-grpc-java-from-client-end-cleanly
    //
    synchronized (this.clientToBroker) {
      ((ClientCallStreamObserver) this.clientToBroker).cancel("Client is closing", null);
    }

    // close the connection with the broker
    this.tqClient.close();

    this.stateMachine.setState(ClientPossibleStates.Idle);
  }

  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("connectionUid",  this.connectionUid)
      .add("state", this.stateMachine.getInternalState())      // desired state
      .add("connState", this.tqClient.getState()) // conn state
      .toString();
  }
}