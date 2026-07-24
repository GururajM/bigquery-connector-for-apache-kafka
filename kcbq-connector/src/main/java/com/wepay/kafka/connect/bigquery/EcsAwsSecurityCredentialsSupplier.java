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

import static com.wepay.kafka.connect.bigquery.utils.GsonUtils.getAsString;

import com.google.auth.oauth2.AwsSecurityCredentials;
import com.google.auth.oauth2.AwsSecurityCredentialsSupplier;
import com.google.auth.oauth2.ExternalAccountSupplierContext;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AwsSecurityCredentialsSupplier} that sources AWS credentials from the ECS/Fargate
 * container credentials endpoint (the relative-URI form).
 *
 * <p>google-auth's built-in AWS provider reads credentials only from environment variables or the
 * EC2 IMDS endpoint; it does not read the ECS/Fargate container-credentials endpoint ({@code
 * 169.254.170.2$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI}), which is the only place Fargate delivers
 * task-role credentials. This supplier plugs that gap for Workload Identity Federation ({@code
 * keySource=WIF_JSON}) without pulling in the AWS SDK and without setting any process-wide
 * environment variables.
 *
 * <p>The supplier is stateless and fetches on demand (no caching): the ECS agent keeps the
 * endpoint's credentials valid and rotates them underneath, and google-auth caches the derived GCP
 * token and only calls back on refresh (~hourly). It is therefore thread-safe with no
 * synchronization; the shared {@link HttpClient} and {@link Gson} are {@code static} (both are safe
 * to share for reads) which also keeps the class cheap to serialize.
 */
public class EcsAwsSecurityCredentialsSupplier implements AwsSecurityCredentialsSupplier {

  private static final Logger logger =
      LoggerFactory.getLogger(EcsAwsSecurityCredentialsSupplier.class);

  private static final long serialVersionUID = 1L;

  /** Default base URI for the ECS/Fargate container credentials endpoint. */
  static final String DEFAULT_BASE_URI = "http://169.254.170.2";

  static final String RELATIVE_URI_ENV = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
  static final String REGION_ENV = "AWS_REGION";
  static final String DEFAULT_REGION_ENV = "AWS_DEFAULT_REGION";

  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

  private static final HttpClient HTTP_CLIENT =
      HttpClient.newBuilder().connectTimeout(REQUEST_TIMEOUT).build();
  private static final Gson GSON = new Gson();

  private final String baseUri;

  public EcsAwsSecurityCredentialsSupplier() {
    this(DEFAULT_BASE_URI);
  }

  /**
   * @param baseUri base URI of the container credentials endpoint; primarily a seam for tests to
   *     point at a local stub. Production uses {@link #DEFAULT_BASE_URI}.
   */
  EcsAwsSecurityCredentialsSupplier(String baseUri) {
    this.baseUri = baseUri;
  }

  @Override
  public String getRegion(ExternalAccountSupplierContext context) throws IOException {
    String source = REGION_ENV;
    String region = trimToNull(getEnv(REGION_ENV));
    if (region == null) {
      source = DEFAULT_REGION_ENV;
      region = trimToNull(getEnv(DEFAULT_REGION_ENV));
    }
    if (region == null) {
      throw new IOException(
          "Could not resolve AWS region: set "
              + REGION_ENV
              + " or "
              + DEFAULT_REGION_ENV
              + " (it is baked into the STS request signature, so there is no safe default).");
    }
    logger.debug("Resolved AWS region {} from {}", region, source);
    return region;
  }

  @Override
  public AwsSecurityCredentials getCredentials(ExternalAccountSupplierContext context)
      throws IOException {
    String relativeUri = trimToNull(getEnv(RELATIVE_URI_ENV));
    if (relativeUri == null) {
      throw new IOException(
          "Environment variable "
              + RELATIVE_URI_ENV
              + " is not set; the ECS/Fargate container "
              + "credentials endpoint is unavailable (is this running on ECS/Fargate?).");
    }

    // Log the base URI only; the relative URI carries a per-task credential path token.
    logger.debug("Fetching AWS container credentials from ECS/Fargate endpoint {}", baseUri);
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(baseUri + relativeUri))
            .timeout(REQUEST_TIMEOUT)
            .GET()
            .build();

    HttpResponse<String> response;
    try {
      response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while fetching AWS container credentials", e);
    }

    if (response.statusCode() != 200) {
      throw new IOException(
          "Failed to fetch AWS container credentials: HTTP " + response.statusCode());
    }

    JsonObject json;
    try {
      json = GSON.fromJson(response.body(), JsonObject.class);
    } catch (JsonSyntaxException e) {
      throw new IOException("AWS container credentials response was not valid JSON", e);
    }
    if (json == null) {
      throw new IOException("AWS container credentials response was not valid JSON");
    }
    String accessKeyId = getAsString(json, "AccessKeyId");
    String secretAccessKey = getAsString(json, "SecretAccessKey");
    String token = getAsString(json, "Token");
    // Token (the STS session token) is required: the ECS/Fargate endpoint only ever serves
    // temporary role credentials, and google-auth must include it when SigV4-signing the
    // GetCallerIdentity request. A missing token means a broken response and would otherwise
    // fail later inside the STS exchange with an opaque signature error, so fail fast here.
    if (accessKeyId == null || secretAccessKey == null || token == null) {
      throw new IOException(
          "AWS container credentials response missing AccessKeyId/SecretAccessKey/Token");
    }
    logger.debug("Obtained temporary AWS credentials from ECS/Fargate container endpoint");
    return new AwsSecurityCredentials(accessKeyId, secretAccessKey, token);
  }

  private static String trimToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  /**
   * Reads an environment variable. Package-private and overridable so tests can supply values
   * without mutating the real process environment.
   */
  String getEnv(String name) {
    return System.getenv(name);
  }
}
