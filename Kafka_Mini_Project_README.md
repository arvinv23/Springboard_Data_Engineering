# Kafka Mini Project: Real-Time Fraud Detection System

This project implements a real-time fraud detection system using Apache Kafka and Python. The system processes a stream of transactions to identify potential fraudulent activities based on predefined criteria.

## Project Architecture

The system consists of two main components:
1. A transaction generator that produces synthetic transaction data
2. A fraud detector that processes the transaction stream and identifies suspicious activities

## Project Structure
```
├── docker-compose.yml
├── docker-compose-kafka.yml
├── detector
│   ├── Dockerfile
│   ├── app.py
│   └── requirements.txt
└── generator
    ├── Dockerfile
    ├── app.py
    ├── transactions.py 
    └── requirements.txt
```

## Prerequisites

- Docker and Docker Compose installed
- Python 3.6 or higher
- Basic understanding of Kafka concepts

## Configuration

### Kafka Cluster Setup (docker-compose.kafka.yml)

```yaml
version: "3"
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

## Application Setup (docker-compose.yml)
```yaml
version: "3"
services:
networks:
  default:
    external:
      name: kafka-network
```

## Getting Started
Create the Kafka network:

```yaml
docker network create kafka-network
Start the Kafka cluster:

docker-compose -f docker-compose.kafka.yml up -d
Start the application services:

docker-compose up -d
```

## Monitoring Transactions
To view the transactions being processed:

```yaml
docker-compose -f docker-compose.kafka.yml exec broker \
    kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic queueing.transactions \
    --from-beginning
```

## Stopping the System
To stop and clean up all containers:

```yaml
docker-compose down
docker-compose -f docker-compose.kafka.yml down
docker network rm kafka-network
```

## Implementation Details

### Transaction Generator
- Produces synthetic transaction data
- Configurable transaction rate
- JSON-formatted messages with source, target, amount, and currency

### Fraud Detector
- Consumes transaction stream
- Applies fraud detection logic
- Separates transactions into legitimate and suspicious streams

## Future Improvements

- Add more sophisticated fraud detection algorithms
- Implement real-time monitoring dashboard
- Add transaction analytics
- Enhance error handling and monitoring


