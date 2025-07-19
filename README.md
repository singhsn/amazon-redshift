# Amazon Redshift Assignment 6 Demo

This Maven project automates **cluster schema creation, data loading, and the three analytics queries** required by Assignment‑6.

## How to build & run

```bash
# compile and assemble an uber‑jar (requires JDK 17+ & Maven 3.9+)
mvn clean package

# provide cluster connection information (or export as env vars)
export REDSHIFT_URL=jdbc:postgresql://example-cluster.abc123.ap-south-1.redshift.amazonaws.com:5439/dev
export REDSHIFT_UID=awsuser
export REDSHIFT_PW=MySecretPass123

# run
java -jar target/amazon-redshift-demo-1.0-SNAPSHOT.jar
```

All TPC‑H DDL/data scripts are embedded under `src/main/resources/ddl` so the jar is self‑sufficient.
