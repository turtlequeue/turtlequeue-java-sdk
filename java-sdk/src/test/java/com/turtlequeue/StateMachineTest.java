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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import org.junit.Test;

import com.turtlequeue.StateMachine;
import com.turtlequeue.ConsumerPossibleStates;

public class StateMachineTest
{
  @Test(timeout = 200)
  public void canSetAndReadState()
  {
    StateMachine<ConsumerPossibleStates> stm = new StateMachine<ConsumerPossibleStates>().setState(ConsumerPossibleStates.Idle);
    assertEquals(ConsumerPossibleStates.Idle, stm.getInternalState());

    stm.setState(ConsumerPossibleStates.Ready);
    assertEquals(ConsumerPossibleStates.Ready, stm.getInternalState());

    stm.setState(ConsumerPossibleStates.Seeking);
    assertEquals(ConsumerPossibleStates.Seeking, stm.getInternalState());

    // TODO play with Completable Futures here?
  }
}