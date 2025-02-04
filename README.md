💊 ops-med

[![MegaLinter](https://github.com/diz-unimr/ops-med/actions/workflows/mega-linter.yml/badge.svg)](https://github.com/diz-unimr/ops-med/actions/workflows/mega-linter.yml)
[![build](https://github.com/diz-unimr/ops-med/actions/workflows/build.yaml/badge.svg)](https://github.com/diz-unimr/ops-med/actions/workflows/build.yaml)
[![docker](https://github.com/diz-unimr/ops-med/actions/workflows/release.yaml/badge.svg)](https://github.com/diz-unimr/ops-med/actions/workflows/release.yaml)
[![codecov](https://codecov.io/gh/diz-unimr/ops-med/graph/badge.svg?token=urFEEfhEJB)](https://codecov.io/gh/diz-unimr/ops-med)


> Kafka processor to map FHIR🔥 medication resources
> from [OPS](https://www.bfarm.de/EN/Code-systems/Classifications/OPS-ICHI/OPS/_node.html) procedure data

## Offset handling

The consumer is configured to auto-commit offsets, in order to improve performance. Successfully processed (and
produced) records are committed manually to the offset store (`enable.auto.offset.store`) to be eligible for
auto-comitting.

## Mapping OPS

Mapping is done for procedures with codes from the OPS medication chapter 6 with a fixed mapping using ATC codes and
ingredient data. Each procedure mapped results in an output bundle containing a `Medication` and a `MedicationStatement`
resource.

## Configuration properties

Application properties are read from a properties file ([app.yaml](./app.yaml)) with default values.

| Name                                | Default                                                                                                        | Description                                   |
|-------------------------------------|----------------------------------------------------------------------------------------------------------------|-----------------------------------------------|
| `app.log_level`                     | info                                                                                                           | Log level (error,warn,info,debug,trace)       |
| `kafka.brokers`                     | localhost:9092                                                                                                 | Kafka brokers                                 |
| `kafka.security_protocol`           | plaintext                                                                                                      | Kafka communication protocol                  |
| `kafka.ssl.ca_location`             | /app/cert/kafka_ca.pem                                                                                         | Kafka CA certificate location                 |
| `kafka.ssl.certificate_location`    | /app/cert/app_cert.pem                                                                                         | Client certificate location                   |
| `kafka.ssl.key_location`            | /app/cert/app_key.pem                                                                                          | Client key location                           |
| `kafka.ssl.key_password`            |                                                                                                                | Client key password                           |
| `kafka.consumer_group`              | ops-med                                                                                                        | Consumer group name                           |
| `kafka.input_topic`                 | test-fhir                                                                                                      | Kafka topic to consume                        |
| `kafka.output_topic`                | test-output                                                                                                    | Kafka output topic                            |
| `kafka.offset_reset`                | earliest                                                                                                       | Kafka consumer reset (`earliest` or `latest`) |
| `fhir.medication.profile`           | https://www.medizininformatik-initiative.de/fhir/core/modul-medikation/StructureDefinition/Medication          | `Medication` FHIR profile                     |
| `fhir.medication.system`            | https://fhir.diz.uni-marburg.de/sid/medication-id                                                              | `Medication` identifier system                |
| `fhir.medication_statement.profile` | https://www.medizininformatik-initiative.de/fhir/core/modul-medikation/StructureDefinition/MedicationStatement | `MedicationStatement` FHIR profile            |
| `fhir.medication_statement.system`  | https://fhir.diz.uni-marburg.de/sid/medication-statement-id                                                    | `MedicationStatement` identifier system       |

### Environment variables

Override configuration properties by providing environment variables with their respective property names.

## License

[AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.en.html)
