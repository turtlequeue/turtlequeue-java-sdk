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

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Executor;

import io.grpc.Status;
import io.grpc.Metadata;
import io.grpc.CallCredentials;
import io.grpc.CallCredentials.RequestInfo;
import io.grpc.CallCredentials.MetadataApplier;

import com.turtlequeue.sdk.api.proto.TurtleQueueGrpc;

// https://grpc.io/docs/guides/auth/
// https://mark-cs.co.uk/posts/2020/july/grpc-call-credentials-in-java/

public class AuthenticationCallCredentials extends CallCredentials {

  private static final Logger logger = Logger.getLogger(TurtleQueueGrpc.class.getName());

  private String userToken;
  private String apiKey;
  private String authMethod;
  private String dataFormat;
  private String sdkVersion;

  static private Metadata.Key<String> META_DATA_USER_TOKEN = Metadata.Key.of("UserToken", Metadata.ASCII_STRING_MARSHALLER);
  static private Metadata.Key<String> META_DATA_API_KEY = Metadata.Key.of("ApiKey", Metadata.ASCII_STRING_MARSHALLER);

  static private Metadata.Key<String> META_DATA_AUTH_METHOD = Metadata.Key.of("AuthMethod", Metadata.ASCII_STRING_MARSHALLER);
  static private Metadata.Key<String> META_DATA_DATA_FORMAT = Metadata.Key.of("DataFormat", Metadata.ASCII_STRING_MARSHALLER);
  static private Metadata.Key<String> META_DATA_SDK_VERSION = Metadata.Key.of("SdkVersion", Metadata.ASCII_STRING_MARSHALLER);

  public AuthenticationCallCredentials(String userToken, String apiKey, String authMethod, String dataFormat, String sdkVersion) {
    this.userToken = userToken;
    this.apiKey = apiKey;

    // optional
    this.authMethod = authMethod;
    this.dataFormat = dataFormat;
    this.sdkVersion = sdkVersion;
  }

  @Override
  public void applyRequestMetadata(RequestInfo requestInfo,
                                   Executor executor,
                                   MetadataApplier metadataApplier) {

    AuthenticationCallCredentials thisRef = this;

    executor.execute(() -> {
        try {
          // called for every admin request but only once per stream
          Metadata headers = new Metadata();

          if(thisRef.authMethod != null) {
            headers.put(META_DATA_AUTH_METHOD, thisRef.authMethod);
          }

          headers.put(META_DATA_USER_TOKEN, thisRef.userToken);
          headers.put(META_DATA_API_KEY, thisRef.apiKey);

          if(thisRef.dataFormat != null) {
            headers.put(META_DATA_DATA_FORMAT, thisRef.dataFormat);
          }

          if(thisRef.sdkVersion != null) {
            headers.put(META_DATA_SDK_VERSION, thisRef.sdkVersion);
          }

          metadataApplier.apply(headers);
        } catch (Throwable e) {
          metadataApplier.fail(Status.UNAUTHENTICATED.withCause(e));
        }
      });
  }

  @Override
  public void thisUsesUnstableApi() {
    // yes this is unstable :(
  }
}