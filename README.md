
<div align="center">

# Kafka Error Handling
### Reliable Message Processing with Retry Topics & Dead Letter Topic (DLT)

*A hands-on Spring Boot project showing how to handle consumer failures in Apache Kafka without losing a single event.*

</div>

---

## Table of Contents

1. [Overview](#1-overview)
2. [The Problem](#2-the-problem)
3. [The Solution](#3-the-solution)
4. [Architecture](#4-architecture)
5. [Processing Flow](#5-processing-flow)
6. [Topics Created Automatically](#6-topics-created-automatically)
7. [Project Structure](#7-project-structure)
8. [Getting Started](#8-getting-started)

---

## 1. Overview

This project is a practical demonstration of error handling in **Apache Kafka** using **Spring Boot**. It simulates a failure inside a consumer and shows how the system recovers by retrying the event and, if it still fails, safely storing it in a **Dead Letter Topic** for later investigation and reprocessing.

**What you will learn**

- Why unhandled consumer errors lead to data loss
- How to retry failed events using non-blocking retry topics
- How to route permanently failed events to a Dead Letter Topic
- How to tune retry behavior (attempts, delays, excluded exceptions)
- How to verify the behavior with a bulk load of 100 events

---

## 2. The Problem

Kafka runs in a distributed way across multiple machines or containers. While a consumer processes an event, something can go wrong: a database connection drops, an external service such as AWS S3 is unavailable, or another infrastructure issue appears.

If the consumer simply throws an exception and moves on, **the event is lost forever**. In critical domains such as financial transactions, this is unacceptable.

| Without error handling | With retry + DLT |
|---|---|
| Failed event is dropped | Failed event is retried automatically |
| No trace of what failed | Permanent failures are stored in the DLT |
| Data loss on temporary outages | Data is preserved and can be reprocessed |

---

## 3. The Solution

The strategy has two layers:

1. **Retry:** when processing fails, Kafka re-attempts the event a configured number of times, each time through a dedicated retry topic.
2. **Dead Letter Topic (DLT):** when every attempt is exhausted, the event is moved to a separate topic that holds all permanently failed events, so nothing is lost and everything can be monitored.

> **Note on attempt count:** the configured value includes the original attempt. Setting it to 4 means 1 original attempt plus **3 retries**. The default is 3.

---

## 4. Architecture

```mermaid
flowchart LR
    subgraph Client
        A([Postman / REST Client])
    end

    subgraph Publisher["Publisher Service"]
        B[REST Controller]
        C[Kafka Producer]
    end

    subgraph Kafka["Apache Kafka Cluster"]
        D[(Main Topic<br/>3 partitions)]
        E[(retry-0)]
        F[(retry-1)]
        G[(retry-2)]
        H[(DLT<br/>Dead Letter Topic)]
    end

    subgraph Consumer["Consumer Service"]
        I{{Validate & Process}}
        J[DLT Handler]
    end

    K[(Database / External Service)]

    A -->|User event| B --> C --> D
    D --> I
    I -->|success| K
    I -.->|fail| E
    E -.->|fail| F
    F -.->|fail| G
    G -.->|all retries exhausted| H
    H --> J
    E -->|success| K
    F -->|success| K
    G -->|success| K

    classDef topic fill:#fff3cd,stroke:#e0a800,stroke-width:2px,color:#333;
    classDef dlt fill:#f8d7da,stroke:#c82333,stroke-width:2px,color:#333;
    classDef svc fill:#d1ecf1,stroke:#17a2b8,stroke-width:2px,color:#333;
    classDef ok fill:#d4edda,stroke:#28a745,stroke-width:2px,color:#333;

    class D,E,F,G topic;
    class H dlt;
    class B,C,I,J svc;
    class K ok;
```

---

## 5. Processing Flow

The sequence below shows what happens to a single event that keeps failing until it reaches the DLT.

```mermaid
sequenceDiagram
    autonumber
    participant P as Publisher
    participant M as Main Topic
    participant C as Consumer
    participant R as Retry Topics (0, 1, 2)
    participant D as Dead Letter Topic

    P->>M: Publish User event
    M->>C: Deliver event (attempt 1)
    C--xC: Processing fails
    C->>R: Forward to retry-0
    R->>C: Deliver event (attempt 2)
    C--xC: Processing fails
    C->>R: Forward to retry-1
    R->>C: Deliver event (attempt 3)
    C--xC: Processing fails
    C->>R: Forward to retry-2
    R->>C: Deliver event (attempt 4)
    C--xC: Processing fails
    C->>D: Retries exhausted, publish to DLT
    D->>C: DLT Handler logs the failed event
    Note over D: Event is stored safely.<br/>Investigate and reprocess later.
```

**Decision logic**

```mermaid
flowchart TD
    S([Event received]) --> V{Processing<br/>successful?}
    V -->|Yes| Done([Done])
    V -->|No| Q{Retries<br/>remaining?}
    Q -->|Yes| RT[Send to next retry topic<br/>after backoff delay] --> V
    Q -->|No| DL[Publish to DLT]
    DL --> INV([Monitor, investigate, reprocess])

    classDef good fill:#d4edda,stroke:#28a745,color:#333;
    classDef bad fill:#f8d7da,stroke:#c82333,color:#333;
    classDef warn fill:#fff3cd,stroke:#e0a800,color:#333;
    class Done good;
    class DL,INV bad;
    class RT warn;
```

---

## 6. Topics Created Automatically

With 4 attempts configured, Kafka creates the following topics without any manual setup:

| Topic | Role |
|---|---|
| Main topic | Receives all new events (created with 3 partitions) |
| Main topic + `-retry-0` | First retry attempt |
| Main topic + `-retry-1` | Second retry attempt |
| Main topic + `-retry-2` | Third retry attempt |
| Main topic + `-dlt` | Final destination for events that failed every attempt |

> The DLT only receives **failed** events. Successfully processed events never reach the retry topics or the DLT.

---

## 7. Project Structure

The repository contains two Spring Boot services:

| Module | Responsibility |
|---|---|
| **Publisher** | Exposes a REST endpoint, accepts a `User` payload, and publishes it to Kafka. Can also load a CSV of users and publish them in bulk. |
| **Consumer** | Listens to the main topic, validates each event, and handles retry and DLT logic. |

**`User` model**

| Field | Description |
|---|---|
| `id` | Unique identifier |
| `firstName` | First name |
| `lastName` | Last name |
| `email` | Email address |
| `gender` | Gender |
| `ipAddress` | Client IP, used for the simulated validation |

**Failure simulation:** the consumer holds a list of restricted IP addresses. When an incoming user has one of those IPs, the consumer raises an exception. This stands in for a real-world failure such as a database or AWS outage.

---

## 8. Getting Started

**Prerequisites**

- JDK 17 or later
- Maven
- Apache Kafka and ZooKeeper (local installation)
- Postman or any REST client
- Offset Explorer (optional, for visualizing topics and messages)

**Run order**

1. Start **ZooKeeper**
2. Start the **Kafka** server
3. (Optional) Connect **Offset Explorer** to the local cluster
4. Start the **Consumer** service
5. Start the **Publisher** service

On startup, the main topic is created with three partitions, and the retry and DLT topics are created automatically. You can confirm this in the application logs and in Offset Explorer.
---

<div align="center">

**Based on the Java Techie tutorial on Kafka error handling with retry and DLT.**

</div>
