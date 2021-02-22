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

import com.turtlequeue.TopicBuilderImpl;


public class TopicBuilderImplTest
{
  @Test
  public void canParseStrings()
  {

    TopicBuilderImpl.fromString("persistent://my-ns/my-topic");
    TopicBuilderImpl.fromString("my-topic");
    TopicBuilderImpl.fromString("non-persistent://my-ns/my-topic");

  }

}