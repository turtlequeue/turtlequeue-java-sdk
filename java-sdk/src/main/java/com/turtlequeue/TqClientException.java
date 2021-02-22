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

import java.util.Map;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import com.google.common.base.MoreObjects;

import com.turtlequeue.sdk.api.proto.Tq.ErrorInfo;
import com.turtlequeue.sdk.api.proto.Tq.ReplyError;


/**
 * Base type of exception for the Turtlequeue SDK.
 * Carries additional data
 */
// was RuntimeException but better make it checked
@SuppressWarnings("serial")
public class TqClientException extends IOException {
  public final String reason; // msg
  public final Map<String, String> data;
  public final String domain;
  private final Long sequenceId;


  public long getSequenceId() {
    if(sequenceId == null) {
      return 0L;
    } else {
      return sequenceId;
    }
  }

  public String getDomain() {
    return this.domain;
  }

  public Map<String, String> getData() {
    return this.data;
  }

  public String getReason() {
    return this.reason;
  }

  public TqClientException(String msg) {
    super(msg);
    this.reason = msg;
    this.domain = "com.turtlequeue";
    this.data = null;
    this.sequenceId = 0L ;
  }

  public TqClientException(String msg, Throwable t) {
    super(msg, t);
    this.reason = msg;
    this.domain = "com.turtlequeue";
    this.data = null;
    this.sequenceId = 0L;
  }

  public TqClientException(ReplyError err) {
    super(err.getErrorInfo().getReason());
    ErrorInfo errorInfo = err.getErrorInfo();
    this.reason = errorInfo.getReason();
    this.domain = errorInfo.getDomain();
    this.data = errorInfo.getMetadataMap();
    this.sequenceId = err.getSequenceId();
  }


  public TqClientException(ReplyError err, Throwable t) {
    super(err.getErrorInfo().getReason(), t);
    ErrorInfo errorInfo = err.getErrorInfo();
    this.reason = errorInfo.getReason();
    this.domain = errorInfo.getDomain();
    this.data = errorInfo.getMetadataMap();
    this.sequenceId = err.getSequenceId();
  }


  protected static TqClientException makeTqExceptionFromReplyError(ReplyError err) {
    switch(err.getKnownError()) {

    case ALREADY_CLOSED_EXCEPTION:
      return new TqClientException.AlreadyClosedException(err);

    case AUTHENTICATION_EXCEPTION:
      return new TqClientException.AuthenticationException(err);

    case AUTHORIZATION_EXCEPTION:
      return new TqClientException.AuthorizationException(err);

    case BROKER_METADATA_EXCEPTION:
      return new TqClientException.BrokerMetadataException(err);

    case BROKER_PERSISTENCE_EXCEPTION:
      return new TqClientException.BrokerPersistenceException(err);

    case CHECKSUM_EXCEPTION:
      return new TqClientException.ChecksumException(err);

    case CONNECT_EXCEPTION:
      return new TqClientException.ConnectException(err);

    case CONSUMER_ASSIGN_EXCEPTION:
      return new TqClientException.ConsumerAssignException(err);

    case CONSUMER_BUSY_EXCEPTION:
      return new TqClientException.ConsumerBusyException(err);

    case CRYPTO_EXCEPTION:
      return new TqClientException.CryptoException(err);

    case GETTING_AUTHENTICATION_DATA_EXCEPTION:
      return new TqClientException.GettingAuthenticationDataException(err);

    case INCOMPATIBLE_SCHEMA_EXCEPTION:
      return new TqClientException.IncompatibleSchemaException(err);

    case INVALID_CONFIGURATION_EXCEPTION:
      return new TqClientException.InvalidConfigurationException(err);

    case INVALID_MESSAGE_EXCEPTION:
      return new TqClientException.InvalidMessageException(err);

    case INVALID_SERVICE_URL:
      return new TqClientException.InvalidServiceURL(err);

    case INVALID_TOPIC_NAME_EXCEPTION:
      return new TqClientException.InvalidTopicNameException(err);

    case LOOKUP_EXCEPTION:
      return new TqClientException.LookupException(err);

    case MESSAGE_ACKNOWLEDGE_EXCEPTION:
      return new TqClientException.MessageAcknowledgeException(err);

    case NOT_ALLOWED_EXCEPTION:
      return new TqClientException.NotAllowedException(err);

    case NOT_CONNECTED_EXCEPTION:
      return new TqClientException.NotConnectedException(err);

    case NOT_FOUND_EXCEPTION:
      return new TqClientException.NotFoundException(err);

    case NOT_SUPPORTED_EXCEPTION:
      return new TqClientException.NotSupportedException(err);

    case PRODUCER_BLOCKED_QUOTA_EXCEEDED_ERROR:
      return new TqClientException.ProducerBlockedQuotaExceededError(err);

    case PRODUCER_BLOCKED_QUOTA_EXCEEDED_EXCEPTION:
      return new TqClientException.ProducerBlockedQuotaExceededException(err);

    case PRODUCER_BUSY_EXCEPTION:
      return new TqClientException.ProducerBusyException(err);

    case PRODUCER_QUEUE_IS_FULL_ERROR:
      return new TqClientException.ProducerQueueIsFullError(err);

    case TIMEOUT_EXCEPTION:
      return new TqClientException.TimeoutException(err);

    case TOPIC_DOES_NOT_EXIST_EXCEPTION:
      return new TqClientException.TopicDoesNotExistException(err);

    case TOPIC_TERMINATED_EXCEPTION:
      return new TqClientException.TopicTerminatedException(err);

    case TRANSACTION_CONFLICT_EXCEPTION:
      return new TqClientException.TransactionConflictException(err);

    case UNSUPPORTED_AUTHENTICATION_EXCEPTION:
      return new TqClientException.UnsupportedAuthenticationException(err);


    default:
      return new TqClientException(err);
    }
  }

  public static class AlreadyClosedException extends TqClientException {

    public AlreadyClosedException(String msg) {
      super(msg);
    }

    public AlreadyClosedException(String msg, Throwable t) {
      super(msg, t);
    }

    public AlreadyClosedException(ReplyError err) {
      super(err);
    }

    public AlreadyClosedException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class AuthenticationException extends TqClientException {

    public AuthenticationException(String msg) {
      super(msg);
    }

    public AuthenticationException(String msg, Throwable t) {
      super(msg, t);
    }

    public AuthenticationException(ReplyError err) {
      super(err);
    }

    public AuthenticationException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class AuthorizationException extends TqClientException {

    public AuthorizationException(String msg) {
      super(msg);
    }

    public AuthorizationException(String msg, Throwable t) {
      super(msg, t);
    }

    public AuthorizationException(ReplyError err) {
      super(err);
    }

    public AuthorizationException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class BrokerMetadataException extends TqClientException {

    public BrokerMetadataException(String msg) {
      super(msg);
    }

    public BrokerMetadataException(String msg, Throwable t) {
      super(msg, t);
    }

    public BrokerMetadataException(ReplyError err) {
      super(err);
    }

    public BrokerMetadataException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class BrokerPersistenceException extends TqClientException {

    public BrokerPersistenceException(String msg) {
      super(msg);
    }

    public BrokerPersistenceException(String msg, Throwable t) {
      super(msg, t);
    }

    public BrokerPersistenceException(ReplyError err) {
      super(err);
    }

    public BrokerPersistenceException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class ChecksumException extends TqClientException {

    public ChecksumException(String msg) {
      super(msg);
    }

    public ChecksumException(String msg, Throwable t) {
      super(msg, t);
    }

    public ChecksumException(ReplyError err) {
      super(err);
    }

    public ChecksumException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class ConnectException extends TqClientException {

    public ConnectException(String msg) {
      super(msg);
    }

    public ConnectException(String msg, Throwable t) {
      super(msg, t);
    }

    public ConnectException(ReplyError err) {
      super(err);
    }

    public ConnectException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class ConsumerAssignException extends TqClientException {

    public ConsumerAssignException(String msg) {
      super(msg);
    }

    public ConsumerAssignException(String msg, Throwable t) {
      super(msg, t);
    }

    public ConsumerAssignException(ReplyError err) {
      super(err);
    }

    public ConsumerAssignException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class ConsumerBusyException extends TqClientException {

    public ConsumerBusyException(String msg) {
      super(msg);
    }

    public ConsumerBusyException(String msg, Throwable t) {
      super(msg, t);
    }

    public ConsumerBusyException(ReplyError err) {
      super(err);
    }

    public ConsumerBusyException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class CryptoException extends TqClientException {

    public CryptoException(String msg) {
      super(msg);
    }

    public CryptoException(String msg, Throwable t) {
      super(msg, t);
    }

    public CryptoException(ReplyError err) {
      super(err);
    }

    public CryptoException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class GettingAuthenticationDataException extends TqClientException {

    public GettingAuthenticationDataException(String msg) {
      super(msg);
    }

    public GettingAuthenticationDataException(String msg, Throwable t) {
      super(msg, t);
    }

    public GettingAuthenticationDataException(ReplyError err) {
      super(err);
    }

    public GettingAuthenticationDataException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class IncompatibleSchemaException extends TqClientException {

    public IncompatibleSchemaException(String msg) {
      super(msg);
    }

    public IncompatibleSchemaException(String msg, Throwable t) {
      super(msg, t);
    }

    public IncompatibleSchemaException(ReplyError err) {
      super(err);
    }

    public IncompatibleSchemaException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class InvalidConfigurationException extends TqClientException {

    public InvalidConfigurationException(String msg) {
      super(msg);
    }

    public InvalidConfigurationException(String msg, Throwable t) {
      super(msg, t);
    }

    public InvalidConfigurationException(ReplyError err) {
      super(err);
    }

    public InvalidConfigurationException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class InvalidMessageException extends TqClientException {

    public InvalidMessageException(String msg) {
      super(msg);
    }

    public InvalidMessageException(String msg, Throwable t) {
      super(msg, t);
    }

    public InvalidMessageException(ReplyError err) {
      super(err);
    }

    public InvalidMessageException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class InvalidServiceURL extends TqClientException {

    public InvalidServiceURL(String msg) {
      super(msg);
    }

    public InvalidServiceURL(String msg, Throwable t) {
      super(msg, t);
    }

    public InvalidServiceURL(ReplyError err) {
      super(err);
    }

    public InvalidServiceURL(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class InvalidTopicNameException extends TqClientException {

    public InvalidTopicNameException(String msg) {
      super(msg);
    }

    public InvalidTopicNameException(String msg, Throwable t) {
      super(msg, t);
    }

    public InvalidTopicNameException(ReplyError err) {
      super(err);
    }

    public InvalidTopicNameException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class LookupException extends TqClientException {

    public LookupException(String msg) {
      super(msg);
    }

    public LookupException(String msg, Throwable t) {
      super(msg, t);
    }

    public LookupException(ReplyError err) {
      super(err);
    }

    public LookupException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class MessageAcknowledgeException extends TqClientException {

    public MessageAcknowledgeException(String msg) {
      super(msg);
    }

    public MessageAcknowledgeException(String msg, Throwable t) {
      super(msg, t);
    }

    public MessageAcknowledgeException(ReplyError err) {
      super(err);
    }

    public MessageAcknowledgeException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class NotAllowedException extends TqClientException {

    public NotAllowedException(String msg) {
      super(msg);
    }

    public NotAllowedException(String msg, Throwable t) {
      super(msg, t);
    }

    public NotAllowedException(ReplyError err) {
      super(err);
    }

    public NotAllowedException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class NotConnectedException extends TqClientException {

    public NotConnectedException(String msg) {
      super(msg);
    }

    public NotConnectedException(String msg, Throwable t) {
      super(msg, t);
    }

    public NotConnectedException(ReplyError err) {
      super(err);
    }

    public NotConnectedException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class NotFoundException extends TqClientException {

    public NotFoundException(String msg) {
      super(msg);
    }

    public NotFoundException(String msg, Throwable t) {
      super(msg, t);
    }

    public NotFoundException(ReplyError err) {
      super(err);
    }

    public NotFoundException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class NotSupportedException extends TqClientException {

    public NotSupportedException(String msg) {
      super(msg);
    }

    public NotSupportedException(String msg, Throwable t) {
      super(msg, t);
    }

    public NotSupportedException(ReplyError err) {
      super(err);
    }

    public NotSupportedException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class ProducerBlockedQuotaExceededError extends TqClientException {

    public ProducerBlockedQuotaExceededError(String msg) {
      super(msg);
    }

    public ProducerBlockedQuotaExceededError(String msg, Throwable t) {
      super(msg, t);
    }

    public ProducerBlockedQuotaExceededError(ReplyError err) {
      super(err);
    }

    public ProducerBlockedQuotaExceededError(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class ProducerBlockedQuotaExceededException extends TqClientException {

    public ProducerBlockedQuotaExceededException(String msg) {
      super(msg);
    }

    public ProducerBlockedQuotaExceededException(String msg, Throwable t) {
      super(msg, t);
    }

    public ProducerBlockedQuotaExceededException(ReplyError err) {
      super(err);
    }

    public ProducerBlockedQuotaExceededException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class ProducerBusyException extends TqClientException {

    public ProducerBusyException(String msg) {
      super(msg);
    }

    public ProducerBusyException(String msg, Throwable t) {
      super(msg, t);
    }

    public ProducerBusyException(ReplyError err) {
      super(err);
    }

    public ProducerBusyException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class ProducerQueueIsFullError extends TqClientException {

    public ProducerQueueIsFullError(String msg) {
      super(msg);
    }

    public ProducerQueueIsFullError(String msg, Throwable t) {
      super(msg, t);
    }

    public ProducerQueueIsFullError(ReplyError err) {
      super(err);
    }

    public ProducerQueueIsFullError(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class TimeoutException extends TqClientException {

    public TimeoutException(String msg) {
      super(msg);
    }

    public TimeoutException(String msg, Throwable t) {
      super(msg, t);
    }

    public TimeoutException(ReplyError err) {
      super(err);
    }

    public TimeoutException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class TopicDoesNotExistException extends TqClientException {

    public TopicDoesNotExistException(String msg) {
      super(msg);
    }

    public TopicDoesNotExistException(String msg, Throwable t) {
      super(msg, t);
    }

    public TopicDoesNotExistException(ReplyError err) {
      super(err);
    }

    public TopicDoesNotExistException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class TopicTerminatedException extends TqClientException {

    public TopicTerminatedException(String msg) {
      super(msg);
    }

    public TopicTerminatedException(String msg, Throwable t) {
      super(msg, t);
    }

    public TopicTerminatedException(ReplyError err) {
      super(err);
    }

    public TopicTerminatedException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class TransactionConflictException extends TqClientException {

    public TransactionConflictException(String msg) {
      super(msg);
    }

    public TransactionConflictException(String msg, Throwable t) {
      super(msg, t);
    }

    public TransactionConflictException(ReplyError err) {
      super(err);
    }

    public TransactionConflictException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static class UnsupportedAuthenticationException extends TqClientException {

    public UnsupportedAuthenticationException(String msg) {
      super(msg);
    }

    public UnsupportedAuthenticationException(String msg, Throwable t) {
      super(msg, t);
    }

    public UnsupportedAuthenticationException(ReplyError err) {
      super(err);
    }

    public UnsupportedAuthenticationException(ReplyError err, Throwable t) {
      super(err, t);
    }
  }

  public static boolean isRetriableError(Throwable t) {
    if (t instanceof AuthorizationException
        || t instanceof InvalidServiceURL
        || t instanceof InvalidConfigurationException
        || t instanceof NotFoundException
        || t instanceof IncompatibleSchemaException
        || t instanceof TopicDoesNotExistException
        || t instanceof UnsupportedAuthenticationException
        || t instanceof InvalidMessageException
        || t instanceof InvalidTopicNameException
        || t instanceof NotSupportedException
        || t instanceof NotAllowedException
        || t instanceof ChecksumException
        || t instanceof CryptoException
        || t instanceof ConsumerAssignException
        || t instanceof MessageAcknowledgeException
        || t instanceof TransactionConflictException
        || t instanceof ProducerBusyException
        || t instanceof ConsumerBusyException) {
      return false;
    }
    return true;
  }
}
