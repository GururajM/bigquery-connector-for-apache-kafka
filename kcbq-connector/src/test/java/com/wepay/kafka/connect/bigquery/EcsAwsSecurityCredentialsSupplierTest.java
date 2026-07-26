/*
 * Copyright 2024 Copyright 2022 Aiven Oy and
 * bigquery-connector-for-apache-kafka project contributors
 *
 * This software contains code derived from the Confluent BigQuery
 * Kafka Connector, Copyright Confluent, Inc, which in turn
 * contains code derived from the WePay BigQuery Kafka Connector,
 * Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.auth.oauth2.AwsSecurityCredentials;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class EcsAwsSecurityCredentialsSupplierTest {

  private static final String RELATIVE_URI = "/v2/credentials/token-abc";

  private HttpServer server;

  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop(0);
      server = null;
    }
  }

  /** Starts a stub container-credentials endpoint returning the given status and body. */
  private String startStub(int status, String body) throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        RELATIVE_URI,
        exchange -> {
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
          }
        });
    server.start();
    return "http://localhost:" + server.getAddress().getPort();
  }

  /** Supplier with an injected env map, so tests never mutate the real process environment. */
  private static EcsAwsSecurityCredentialsSupplier supplier(
      String baseUri, Map<String, String> env) {
    return new EcsAwsSecurityCredentialsSupplier(baseUri) {
      @Override
      String getEnv(String name) {
        return env.get(name);
      }
    };
  }

  private static Map<String, String> envWithRelativeUri() {
    Map<String, String> env = new HashMap<>();
    env.put(EcsAwsSecurityCredentialsSupplier.RELATIVE_URI_ENV, RELATIVE_URI);
    return env;
  }

  @Test
  public void getCredentialsReturnsParsedCredentials() throws IOException {
    String baseUri =
        startStub(
            200, "{\"AccessKeyId\":\"AKID\",\"SecretAccessKey\":\"SECRET\",\"Token\":\"TOKEN\"}");
    AwsSecurityCredentials creds = supplier(baseUri, envWithRelativeUri()).getCredentials(null);
    assertEquals("AKID", creds.getAccessKeyId());
    assertEquals("SECRET", creds.getSecretAccessKey());
    assertEquals("TOKEN", creds.getSessionToken());
  }

  @Test
  public void getCredentialsThrowsOnNon200() throws IOException {
    String baseUri = startStub(500, "boom");
    assertThrows(
        IOException.class, () -> supplier(baseUri, envWithRelativeUri()).getCredentials(null));
  }

  @Test
  public void getCredentialsThrowsWhenRelativeUriEnvMissing() {
    assertThrows(
        IOException.class,
        () -> supplier("http://localhost:1", new HashMap<>()).getCredentials(null));
  }

  @Test
  public void getCredentialsThrowsWhenTokenMissing() throws IOException {
    String baseUri = startStub(200, "{\"AccessKeyId\":\"AKID\",\"SecretAccessKey\":\"SECRET\"}");
    assertThrows(
        IOException.class, () -> supplier(baseUri, envWithRelativeUri()).getCredentials(null));
  }

  @Test
  public void getCredentialsThrowsOnMalformedJson() throws IOException {
    String baseUri = startStub(200, "not json");
    assertThrows(
        IOException.class, () -> supplier(baseUri, envWithRelativeUri()).getCredentials(null));
  }

  @Test
  public void getCredentialsThrowsOnConnectionRefused() throws IOException {
    // A port with nothing listening: the fetch must surface as an IOException.
    int deadPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      deadPort = socket.getLocalPort();
    }
    assertThrows(
        IOException.class,
        () -> supplier("http://localhost:" + deadPort, envWithRelativeUri()).getCredentials(null));
  }

  @Test
  public void getRegionPrefersAwsRegion() throws IOException {
    Map<String, String> env = new HashMap<>();
    env.put(EcsAwsSecurityCredentialsSupplier.REGION_ENV, "us-east-1");
    env.put(EcsAwsSecurityCredentialsSupplier.DEFAULT_REGION_ENV, "eu-west-1");
    assertEquals("us-east-1", supplier("http://unused", env).getRegion(null));
  }

  @Test
  public void getRegionFallsBackToDefaultRegion() throws IOException {
    Map<String, String> env = new HashMap<>();
    env.put(EcsAwsSecurityCredentialsSupplier.DEFAULT_REGION_ENV, "eu-west-1");
    assertEquals("eu-west-1", supplier("http://unused", env).getRegion(null));
  }

  @Test
  public void getRegionThrowsWhenUnset() {
    assertThrows(
        IOException.class, () -> supplier("http://unused", new HashMap<>()).getRegion(null));
  }

  @Test
  public void getRegionTrimsWhitespace() throws IOException {
    Map<String, String> env = new HashMap<>();
    env.put(EcsAwsSecurityCredentialsSupplier.REGION_ENV, "  us-east-1  ");
    assertEquals("us-east-1", supplier("http://unused", env).getRegion(null));
  }
}
