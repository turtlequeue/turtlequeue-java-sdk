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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

// TODO some of these should be synchronized on the state?
//   -> use AtomicReference
//  https://github.com/apache/pulsar/blob/281163b26f3b842c0834ff46b481453cddea7860/pulsar-client/src/main/java/org/apache/pulsar/client/impl/HandlerState.java

// TODO @ThreadSafe

public class StateMachine<T> {


  private AtomicReference<T> state = null;

  StateMachine() {
    this.state = new AtomicReference<T>();
  }

  protected T getInternalState() {
    return state.get();
  }

  protected StateMachine<T> setState(T st) {
    state.set(st);
    return this;
  }

  protected T getAndUpdateState(final UnaryOperator<T> updater) {
    return state.getAndUpdate(updater);
  }

  protected boolean compareAndSet(T prev, T next) {
    return state.compareAndSet(prev, next);
  }

  protected void checkStateIs(T st, String desc) {
    if(this.state.get() != st) {
      throw new IllegalStateException(desc);
    }
  }
}
