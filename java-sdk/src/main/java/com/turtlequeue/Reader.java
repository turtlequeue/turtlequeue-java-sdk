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
// http://pulsar.apache.org/api/client/2.2.0/org/apache/pulsar/client/impl/ReaderImpl.html

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.turtlequeue.Topic;

/**
 * A Reader can be used to scan through all the messages currently available in a topic.
 */
public interface Reader<T> extends Closeable {

    /**
     * @return the topic from which this reader is reading from
     */
    Topic getTopic();

    /**
     * Read asynchronously the next message in the topic.
     *
     * <p>{@code readNextAsync()} should be called subsequently once returned {@code CompletableFuture} gets complete
     * with received message. Else it creates <i> backlog of receive requests </i> in the application.
     *
     * <p>The returned future can be cancelled before completion by calling {@code .cancel(false)}
     * ({@link CompletableFuture#cancel(boolean)}) to remove it from the the backlog of receive requests. Another
     * choice for ensuring a proper clean up of the returned future is to use the CompletableFuture.orTimeout method
     * which is available on JDK9+. That would remove it from the backlog of receive requests if receiving exceeds
     * the timeout.
     *
     * @return a future that will yield a message (when it's available) or {@link PulsarClientException} if the reader
     *         is already closed.
     */
    CompletableFuture<Message<T>> readNext();

    /**
     * Asynchronously close the reader and stop the broker to push more messages.
     *
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Return true if the topic was terminated and this reader has reached the end of the topic.
     *
     * <p>Note that this only applies to a "terminated" topic (where the topic is "sealed" and no
     * more messages can be published) and not just that the reader is simply caught up with
     * the publishers. Use {@link #hasMessageAvailable()} to check for for that.
     */
    boolean hasReachedEndOfTopic();

    /**
     * Asynchronously check if there is any message available to read from the current position.
     *
     * <p>This check can be used by an application to scan through a topic and stop when the reader reaches the current
     * last published message.
     *
     * <p>Even if this call returns true, that will not guarantee that a subsequent call to {@link #readNext()}
     * will not block.
     * @return a future that will yield true if the are messages available to be read, false otherwise, or a
     *         {@link PulsarClientException} if there was any error in the operation
     */
    CompletableFuture<Boolean> hasMessageAvailable();

    /**
     * @return Whether the reader is connected to the broker
     */
    boolean isConnected();

    /**
     * Reset the subscription associated with this reader to a specific message id.
     *
     * <p>The message id can either be a specific message or represent the first or last messages in the topic.
     * <ul>
     * <li><code>MessageId.earliest</code> : Reset the reader on the earliest message available in the topic
     * <li><code>MessageId.latest</code> : Reset the reader on the latest message in the topic
     * </ul>
     *
     * <p>Note: this operation can only be done on non-partitioned topics. For these, one can rather perform
     * the seek() on the individual partitions.
     *
     * @param messageId the message id where to position the reader
     * @return a future to track the completion of the seek operation
     */
    CompletableFuture<Void> seek(MessageId messageId);

    /**
     * Reset the subscription associated with this reader to a specific message publish time.
     *
     * <p>Note: this operation can only be done on non-partitioned topics. For these, one can rather perform
     * the seek() on the individual partitions.
     *
     * @param timestamp
     *            the message publish time where to position the reader
     * @return a future to track the completion of the seek operation
     */
    CompletableFuture<Void> seek(long timestamp);
}