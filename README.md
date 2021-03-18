## Query Metric Service

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave-query-metric-service/workflows/Tests/badge.svg)

The Query Metric service allows query services to store and update query metrics.
Callers must have the Administrator role to call either updateMetric operations.
When running multiple service instances and using a Hazelcast cache, successive metric updates 
can be made on any of the service instances.

Any user can call the id/{queryId} operation, but users who do not have the metric admin role
can only retrieve a query metric for a query that they ran.

### Query Metric API V1

*https://host:port/querymetrics/v1/*

| Method | Operation     | Description                           | Request Body                |
|:---    |:---           |:---                                   |:---                         |
| `POST` | updateMetric  | Update a single BaseQueryMetric       | BaseQueryMetric             |
| `POST` | updateMetrics | Update a list of BaseQueryMetric      | List&lt;BaseQueryMetric&gt; |
| `GET`  | id/{queryId}  | Retrieve a BaseQueryMetric by queryId | N/A                         |

* See [QueryMetricOperations](src/main/java/datawave/microservice/querymetrics/QueryMetricOperations.java)
  class for details

---

### Getting Started

1. First, refer to [services/README](https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/README.md#getting-started)
   for launching the config service.

2. Launch this service as follows:

   ```
   java -jar target/query-metric-service*-exec.jar --spring.profiles.active=dev
   ```

   See [sample_configuration/querymetrics-dev.yml.example](https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/sample_configuration/querymetrics-dev.yml.example) 
   and configure as desired.

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0
