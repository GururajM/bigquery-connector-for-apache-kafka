# Kafka Connect BigQuery Connector


[![Build site and deploy](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/actions/workflows/build_site.yml/badge.svg)](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/actions/workflows/build_site.yml)

This is an implementation of a sink connector from [Apache Kafka](http://kafka.apache.org) to 
[Google BigQuery](https://cloud.google.com/bigquery/), built on top 
of [Apache Kafka Connect](https://kafka.apache.org/documentation.html#connect).

## Documentation

The Kafka Connect BigQuery Connector documentation is available online at https://aiven-open.github.io/bigquery-connector-for-apache-kafka/.
The site contains a complete list of the configuration options as well as information about the project.

### Configuration notes

If the configuration includes a JSON GCP credential structure that uses a `credential_source` entry, one of the following environment variables must be set.

| Source Type | Environment Variable            |
|-------------|---------------------------------|
| file        | io.aiven.commons.envcheck.files |
| url         | io.aiven.commons.envcheck.uri   |
| executable  | io.aiven.commons.envcheck.cmd   |

The environment variables contain a comma separated list of valid entries for each type.  If the environment variable is not set, or the JSON value is not found in the environment variable, the value will be prohibited and an exception thrown before the connector starts. 

As an example, to access https://example.com/credentials.cgi the environment variable `io.aiven.commons.envcheck.uri` would need to contain the URL:

```
export io.aiven.commons.envcheck.uri=https://example.com/credentials.cgi
# start the kafka processes
```

To add an additional URL, for example `https://example.net/credentials.cgi` the export would look like:

```
export io.aiven.commons.envcheck.uri=https://example.com/credentials.cgi,https://example.net/credentials.cgi 
# start the kafka processes
```


## History

This connector was [originally developed by WePay](https://github.com/wepay/kafka-connect-bigquery).
In late 2020 the project moved to [Confluent](https://github.com/confluentinc/kafka-connect-bigquery),
with both companies taking on maintenance duties.
In 2024, Aiven created [its own fork](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/)
based off the Confluent project in order to continue maintaining an open source, Apache 2-licensed
version of the connector.

## Configuration

### Sample

A simple example connector configuration, that reads records from Kafka with
JSON-encoded values and writes their values to BigQuery:

```json
{
  "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
  "topics": "users, clicks, payments",
  "tasks.max": "3",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",

  "project": "kafka-ingest-testing",
  "defaultDataset": "kcbq-example",
  "keyfile": "/tmp/bigquery-credentials.json"
}
```

### Workload Identity Federation (WIF_JSON)

Setting `keySource` to `WIF_JSON` lets the connector authenticate to GCP via
[Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation)
instead of a static service-account key. It is intended for connectors running on **AWS ECS
Fargate**: the AWS task role is exchanged for a short-lived GCP token, so there is no long-lived
key to store or rotate. The `keyfile` then holds the raw JSON of an `external_account` credential
configuration rather than a service-account key.

Only AWS external accounts are supported today. The connector reads the AWS task-role credentials
from the ECS/Fargate container-credentials endpoint, so it works where google-auth's built-in AWS
provider does not.

Connector configuration:

```json
{
  "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
  "topics": "users, clicks, payments",
  "tasks.max": "3",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",

  "project": "kafka-ingest-testing",
  "defaultDataset": "kcbq-example",
  "keySource": "WIF_JSON",
  "keyfile": "{ ... external_account JSON, as a single string ... }"
}
```

Example `external_account` keyfile — note there is **no `credential_source`**: the connector
supplies the AWS credentials itself, and omitting `credential_source` keeps the keyfile within the
connector's URL allowlist. Replace the `<...>` placeholders:

```json
{
  "type": "external_account",
  "audience": "//iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<POOL_ID>/providers/<PROVIDER_ID>",
  "subject_token_type": "urn:ietf:params:aws:token-type:aws4_request",
  "token_url": "https://sts.googleapis.com/v1/token",
  "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/<SA_EMAIL>:generateAccessToken"
}
```

**AWS/GCP setup prerequisites** (configured outside the connector):

- The Workload Identity Pool **AWS provider** attribute mapping must keep `google.subject` ≤ 127
  bytes — map it to the normalized role ARN (`arn:aws:iam::<ACCOUNT>:role/<ROLE>`), not the full
  assumed-role ARN with session name.
- Grant `roles/iam.workloadIdentityUser` on the target service account, bound to
  `principalSet://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<POOL_ID>/attribute.aws_role/arn:aws:sts::<ACCOUNT>:assumed-role/<ROLE>`.
- The service account needs `roles/bigquery.dataEditor` and `roles/bigquery.jobUser`.
- On Fargate, `AWS_REGION` (or `AWS_DEFAULT_REGION`) must be set — it is by default on Fargate — as
  the region is part of the request signature.

#### Verifying on ECS Fargate

There is no automated integration test for the AWS→GCP path: it only works inside a Fargate task
against a configured GCP Workload Identity Pool. Verify a deployment manually:

1. Deploy the connector on Fargate with `keySource=WIF_JSON` and the `external_account` keyfile above.
2. Produce records to the configured topic(s).
3. Confirm rows appear in BigQuery and consumer offsets advance.
4. **Let it run past ~6 hours** to confirm AWS credential rotation is handled: the sink keeps
   writing with no `Unable to refresh sourceCredentials` errors. The connector fetches fresh AWS
   credentials from the container endpoint on demand; enable `DEBUG` logging to observe
   `Obtained temporary AWS credentials from ECS/Fargate container endpoint`.

### Complete docs
See the [configuration documentation](https://aiven-open.github.io/bigquery-connector-for-apache-kafka/configuration.html) for a list of the connector's
configuration properties.

## Download

Download information is available on the [project web site]((https://aiven-open.github.io/bigquery-connector-for-apache-kafka)). 

## Building from source

This project uses the Maven build tool.

To compile the project without running the integration tests execute `mvn package -DskipITs`.

To build the documentation execute the following steps:

```
mvn install -DskipITs
mvn -f tools
mvn -f docs
```

Once the documentation is built it can be run by executing `mvn -f docs site:run`.

### Integration test setup

Integration tests require a live BigQuery and Kafka installation.  Configuring those components is beyond the scope of this document.

Once you have the test environment ready, integration specific environment variables must be set.

#### Local configuration

- GOOGLE_APPLICATION_CREDENTIALS - the path to a json file that was download when the GCP account key was created.
- KCBQ_TEST_BUCKET - the name of the bucket to use for testing,
- KCBQ_TEST_DATASET - the name of the dataset to use for testing,
- KCBQ_TEST_KEYFILE - same as the GOOGLE_APPLICATION_CREDENTIALS
- KCBQ_TEST_PROJECT - the name of the project to use.  

#### GitHub configuration

To run the integration tests from a GitHub action the following variables must be set

- GCP_CREDENTIALS - the contents of a json file that was download when the GCP account key was created.
- KCBQ_TEST_BUCKET - the bucket to use for the tests
- KCBQ_TEST_DATASET - the data set to use for the tests.
- KCBQ_TEST_PROJECT - the project to use for the tests.
