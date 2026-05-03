# 🏦 Midas Core — JPMorgan Chase Advanced Software Engineering

![Java](https://img.shields.io/badge/Java-17-orange?style=flat-square&logo=java)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.5-brightgreen?style=flat-square&logo=springboot)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6.2-black?style=flat-square&logo=apachekafka)
![H2 Database](https://img.shields.io/badge/H2-In--Memory%20DB-blue?style=flat-square)
![Maven](https://img.shields.io/badge/Maven-Build%20Tool-red?style=flat-square&logo=apachemaven)
![Forage](https://img.shields.io/badge/Forage-JPMorgan%20Chase-blue?style=flat-square)

> Project repo for the **JPMorgan Chase Advanced Software Engineering** virtual internship on Forage.  
> Built a real-world **transaction processing microservice** integrating Kafka, JPA, REST APIs and more.

---

## 📌 Project Overview

**Midas Core** is a Spring Boot microservice that processes financial transactions in real time.  
It listens to a Kafka topic, validates each transaction, persists it to a database, calls an external Incentive API, and exposes a REST endpoint to query user balances.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      MIDAS CORE                         │
│                                                         │
│  Kafka Topic         KafkaConsumer                      │
│  (trader-updates) ──► @KafkaListener                   │
│                           │                             │
│                           ▼                             │
│                    Validate Transaction                  │
│                    (sender exists?                       │
│                     recipient exists?                    │
│                     balance >= amount?)                  │
│                           │                             │
│                           ▼                             │
│                    Call Incentive API ──► localhost:8080 │
│                    (RestTemplate POST)                   │
│                           │                             │
│                           ▼                             │
│                    Update Balances                       │
│                    Save TransactionRecord                │
│                    (H2 + Spring Data JPA)               │
│                                                         │
│  GET /balance ◄── TransactionController                 │
│  (port 33400)      @RestController                      │
└─────────────────────────────────────────────────────────┘
```

---

## ✅ Tasks Completed

### Task 1 — Project Setup
- Forked and cloned the repository
- Added required Maven dependencies to `pom.xml`
- Configured `application.yml` with Kafka topic and serializers
- Fixed Kafka `JsonSerializer` for sending `Transaction` objects

### Task 2 — Kafka Integration
- Implemented `KafkaConsumer` using `@KafkaListener`
- Configured `JsonDeserializer` to convert Kafka JSON messages into `Transaction` objects
- Used embedded Kafka for testing (no external server needed)

### Task 3 — H2 Database Integration
- Created `TransactionRecord` JPA entity with `@ManyToOne` relationships
- Implemented `TransactionRecordRepository` extending `CrudRepository`
- Added transaction validation logic (sender/recipient exist + sufficient balance)
- Used `@Transactional` to ensure data consistency
- Persisted valid transactions and updated user balances

### Task 4 — External REST API Integration
- Ran the provided Incentive API JAR on port 8080
- Used `RestTemplate` to POST transactions to `/incentive` endpoint
- Added incentive amount to recipient's balance
- Stored incentive alongside transaction record

### Task 5 — REST API Controller
- Created `TransactionController` with `GET /balance` endpoint
- Accepts `userId` as request parameter
- Returns JSON-serialized `Balance` object
- Configured app to run on port **33400**

---

## 🛠️ Tech Stack

| Technology | Purpose |
|---|---|
| **Java 17** | Primary language |
| **Spring Boot 3.2.5** | Application framework |
| **Apache Kafka** | Message streaming (producer + consumer) |
| **Spring Data JPA** | Database ORM |
| **H2 Database** | In-memory database for testing |
| **RestTemplate** | HTTP client for external API calls |
| **Maven** | Build and dependency management |
| **JUnit** | Automated testing |

---

## 🚀 How to Run

### Prerequisites
- Java 17+
- Maven

### Steps

**1. Clone the repository**
```bash
git clone https://github.com/kunalraidas/forage-midas.git
cd forage-midas
```

**2. Start the Incentive API (required for Task 4 & 5)**
```bash
cd services
java -jar transaction-incentive-api.jar
```

**3. Build and run the application**
```bash
mvn clean install
mvn spring-boot:run
```

**4. Run specific tests**
```bash
# Run all tests
mvn test

# Run individual task tests
mvn -Dtest=TaskOneTests test
mvn -Dtest=TaskTwoTests test
mvn -Dtest=TaskThreeTests test
mvn -Dtest=TaskFourTests test
mvn -Dtest=TaskFiveTests test
```

---

## 📡 API Endpoint

### GET /balance
Returns the current balance of a user.

**Request:**
```
GET http://localhost:33400/balance?userId=1
```

**Response:**
```json
{
  "amount": 627.50
}
```

**If user not found:**
```json
{
  "amount": 0.0
}
```

---

## ⚙️ Configuration

`application.yml`
```yaml
server:
  port: 33400

general:
  kafka-topic: trader-updates

spring:
  datasource:
    url: jdbc:h2:mem:testdb
    driver-class-name: org.h2.Driver
  jpa:
    hibernate:
      ddl-auto: create-drop
    show-sql: true
  kafka:
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      group-id: midas-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
```

---

## 📁 Project Structure

```
src/
├── main/java/com/jpmc/midascore/
│   ├── entity/
│   │   ├── TransactionRecord.java   # JPA entity for DB persistence
│   │   └── UserRecord.java          # JPA entity for users
│   ├── foundation/
│   │   ├── Transaction.java         # Kafka message model
│   │   ├── Balance.java             # REST response model
│   │   └── Incentive.java           # Incentive API response model
│   ├── repository/
│   │   ├── TransactionRecordRepository.java
│   │   └── UserRepository.java
│   ├── KafkaConsumer.java           # Kafka listener + business logic
│   ├── TransactionController.java   # REST API controller
│   └── MidasCoreApplication.java    # Spring Boot entry point
└── test/
    ├── TaskOneTests.java
    ├── TaskTwoTests.java
    ├── TaskThreeTests.java
    ├── TaskFourTests.java
    └── TaskFiveTests.java
```

---

## 💡 Key Concepts Learned

- **Event-Driven Architecture** — Kafka producer/consumer decoupling
- **Serialization/Deserialization** — JSON conversion for Kafka messages
- **Spring Data JPA** — ORM, entity relationships, repository pattern
- **REST API Design** — Building and consuming REST endpoints
- **Loose Coupling** — Microservice boundaries and API contracts
- **Data Consistency** — `@Transactional` and correct order of operations
- **Embedded Kafka** — Self-contained testing without external infrastructure

---

## 🏆 Certificate

Completed the **JPMorgan Chase Advanced Software Engineering** virtual experience on [Forage](https://www.theforage.com/simulations/jpmorgan/advanced-software-engineering-r0fm).

---

## 👤 Author

**Kunal Raidas**  
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=flat-square&logo=linkedin)](https://linkedin.com/in/kunal-raidas)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black?style=flat-square&logo=github)](https://github.com/kunalraidas)

