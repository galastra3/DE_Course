# Welcome to DE COURSE

## Presentations and Excercises
Are in `\presentations` folder.

## Installations
The relevant `docker-compose` files are in `\infrastracture`.

**NOTE** that JVM needs to be installed on the computer.

### How to install
**Guide for docker compose:** [guide](https://docs.docker.com/compose/gettingstarted/)
TL;DR : `docker compose up` in the directory of the `docker-compose.yml` file.

#### NiFi
- Pull the image from [docker hub](https://hub.docker.com/r/apache/nifi)
- Continue with the guide in the link.

#### Spark
- Run the `docker-compose`.

#### Scala
- JVM is a requirements.
- Can be installed with Intellij.

#### Kafka
- Run the `docker-compose`.
- **TODO**: kafka-connect

#### Trino
- Pull the [image](https://hub.docker.com/r/trinodb/trino) and run it according to the guide.

#### Postgres (pg)
- Pull the [image](https://hub.docker.com/_/postgres) and run it according to the guide.
