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

import com.google.common.collect.Maps;
import com.turtlequeue.TqClientException.AlreadyClosedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import org.junit.Test;

import com.turtlequeue.TqClientException;
import com.turtlequeue.sdk.api.proto.Tq.ErrorInfo;
import com.turtlequeue.sdk.api.proto.Tq.ReplyError;
import com.turtlequeue.sdk.api.proto.Tq.ReplyError.KnownError;

public class TqClientExceptionTest
{
  @Test(timeout = 500)
  public void canBeConstructedFromReplyError()
  {
    ReplyError genericErr = ReplyError.newBuilder()
      .build();

    TqClientException ex = new TqClientException(genericErr);
    assertNull(ex.getCause());

    Exception cause = new Exception("I am the cause");
    TqClientException ex2 = new TqClientException(genericErr, cause);
    assertEquals(0L, ex2.getSequenceId());
  }


  @Test(timeout = 500)
  public void canDetectSpecificErrorCode()
  {
    KnownError c = KnownError.ALREADY_CLOSED_EXCEPTION;
    ReplyError err = ReplyError.newBuilder()
      .setSequenceId(123)
      .setErrorInfo(ErrorInfo.newBuilder()
                    .build())
      .setKnownError(c)
      .build();

    TqClientException res = TqClientException.makeTqExceptionFromReplyError(err);
    assertTrue(res instanceof TqClientException.AlreadyClosedException);
  }


  @Test(timeout = 500)
  public void canFallbackToGenericClass()
  {
    ReplyError err = ReplyError.newBuilder()
      .setSequenceId(123)
      .setErrorInfo(ErrorInfo.newBuilder()
                    .build())
      .build();

    TqClientException res = TqClientException.makeTqExceptionFromReplyError(err);
    assertTrue(res instanceof TqClientException);
  }

}