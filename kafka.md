# 📦 Kafka — Notes for Humans

> Simple, end-to-end notes on Apache Kafka — what problem it solves, how it works, and why it matters.

---

## 🔴 The Problem — Life Before Kafka

Imagine you're building something like **Swiggy or Uber**.

Every driver sends their **GPS location to the server every second**.

```
Driver → Location Update → Server → Write to DB
         (every second)
```

Now imagine **10,000 drivers** doing this simultaneously. That's **10,000 writes per second** to your database.

**What goes wrong?**

- The database gets overwhelmed
- Writes start failing or slowing down
- Your app becomes unreliable

This is called a **DB throughput problem** — the database simply can't keep up with the flood of incoming writes.

---

## ✅ Why Kafka?

Kafka acts as a **buffer** between your data producers (drivers) and your data consumers (services that process the data).

Instead of writing directly to the DB every second, drivers write to **Kafka** — which is extremely fast at accepting data. Then your services read from Kafka and write to the DB in **bulk**, at their own pace.

```
Driver → Kafka → [Fare Service, ETA Service, Analytics] → DB (bulk insert)
```

**The key idea:**  
Kafka **decouples** data ingestion from data processing.

---

## ⚖️ Kafka vs Database — Quick Comparison

| Feature                  | Kafka                    | Database                    |
| ------------------------ | ------------------------ | --------------------------- |
| Throughput (write speed) | 🔥 Very High             | 🐢 Low                      |
| Storage                  | Limited                  | Large                       |
| Querying data            | ❌ Not great             | ✅ Excellent                |
| Persistence              | Temporary (configurable) | Permanent                   |
| Use case                 | Real-time data streams   | Long-term storage & queries |

**Bottom line:** Kafka and a database are not competitors — they work together. Kafka handles the fire hose of incoming data; the database stores it cleanly for later use.

---

## 🧠 Core Kafka Concepts

### 📨 Message

A **message** is the smallest unit of data in Kafka — like a single row of data.

Example: A driver's location update

```json
{
  "driver_id": "D123",
  "lat": 28.6139,
  "lng": 77.209,
  "timestamp": "2024-01-01T10:00:00Z"
}
```

Kafka stores this as a **byte array** internally. You can push JSON, strings, numbers — anything.

Messages can also have a **Key** — a piece of metadata that helps Kafka decide which partition to send the message to (more on this below).

---

### 📂 Topic

A **topic** is a named category for messages — like a folder or a table.

Example:

- `driver-locations` → all GPS updates from drivers
- `order-events` → all order placed/cancelled events
- `payments` → all payment transactions

```
Producer → [driver-locations topic] → Consumers
```

Different types of data go into different topics. Simple as that.

---

### 🗂️ Partition

A **partition** is how Kafka scales.

If one topic gets millions of messages per second, a single machine can't handle it. So Kafka **splits the topic into smaller chunks** called partitions, spread across multiple machines.

Think of a topic as a big array. Partitions are that array split into smaller arrays across machines:

```
driver-locations topic
├── Partition 0: [msg1, msg4, msg7 ...]
├── Partition 1: [msg2, msg5, msg8 ...]
└── Partition 2: [msg3, msg6, msg9 ...]
```

Each message in a partition has an **offset** — its index/position (0, 1, 2, 3...). This is how consumers track what they've already read.

**Key rule:** Messages within a single partition are always in order. But across partitions, order is not guaranteed.

---

### 🏭 Producer

A **producer** is anything that **sends messages** to Kafka — your driver's app, your order service, etc.

The producer decides which partition to send a message to:

| Scenario            | How partition is chosen                                         |
| ------------------- | --------------------------------------------------------------- |
| No key provided     | Random partition (load balanced)                                |
| Key provided        | Consistent hashing → same key always goes to the same partition |
| Partition hardcoded | Goes directly there                                             |

**Why does the key matter?**  
If driver `D123` always sends to the same partition, all their messages stay in order. This is critical for things like tracking a route correctly.

---

### 🛒 Consumer

A **consumer** is anything that **reads messages** from Kafka — your fare calculation service, ETA service, analytics service, etc.

Consumers read messages in order from a partition (offset 0, then 1, then 2...).

The consumer saves its **current offset** so if it crashes and restarts, it knows exactly where to pick up from. No messages are lost.

Consumers can also **rewind** to an older offset if they need to reprocess past data.

---

### 👥 Consumer Group

A **consumer group** is a team of consumers working together to read from a topic.

**The golden rule:**  
One partition can only be read by **one consumer** in a group at a time. This keeps messages in order and prevents duplicate processing.

```
Topic: driver-locations (3 partitions)

Consumer Group A (Fare Service):
├── Consumer 1 → reads Partition 0
├── Consumer 2 → reads Partition 1
└── Consumer 3 → reads Partition 2
```

**What if you have more consumers than partitions?**  
The extra consumer sits idle. Having 4 consumers for 3 partitions means 1 consumer does nothing.

**What if a consumer crashes?**  
Kafka automatically reassigns its partition to another consumer in the group. This is called **rebalancing**.

---

### 🔁 Fan-Out (Pub/Sub Pattern)

Multiple **consumer groups** can read from the same topic independently. Each group gets all the messages.

```
Topic: driver-locations
         │
         ├──► Consumer Group: Fare Service     → calculates ride cost
         ├──► Consumer Group: ETA Service      → calculates arrival time
         └──► Consumer Group: Analytics        → stores data for reports
```

Every group processes the data in its own way. This is called **Pub/Sub** (Publish-Subscribe).

---

### 🖥️ Broker

A **broker** is a single Kafka server/machine.

It:

- Receives messages from producers
- Assigns offsets to messages
- Stores messages to disk (this is why Kafka is durable)
- Serves messages to consumers

---

### 🏙️ Cluster

A **Kafka cluster** is a group of brokers working together.

Benefits:

- **Scalability** — spread the load across machines
- **Fault tolerance** — if one broker dies, others take over
- **High availability** — data is replicated across brokers

One broker in the cluster acts as the **Controller** — it manages partition assignment and monitors broker health.

**Replication:**  
Each partition is copied to multiple brokers (based on replication factor). If your replication factor is 3, each partition lives on 3 different machines.

One broker is the **leader** for a partition (handles all reads/writes). The others are **followers** (just keep a copy). If the leader dies, a follower becomes the new leader automatically.

---

### 🐘 Zookeeper

Kafka uses **Zookeeper** to manage cluster metadata:

- Which brokers are alive
- Who is the leader for each partition
- Topic and partition configuration

> 📝 Note: Newer versions of Kafka are moving away from Zookeeper (called KRaft mode), but Zookeeper is still widely used.

---

## 🗺️ Full Picture — Uber Example

```
10,000 Drivers (Producers)
        │
        ▼
   Kafka Cluster
   ┌─────────────────────────────┐
   │  Topic: driver-locations    │
   │  ├── Partition 0            │
   │  ├── Partition 1            │
   │  └── Partition 2            │
   └─────────────────────────────┘
        │
        ├──► Fare Calculation Service  → bulk write to DB
        ├──► ETA Service               → bulk write to DB
        └──► Analytics Service         → bulk write to Data Warehouse
```

No more flooding the DB with 10,000 writes/second. Kafka absorbs the load and lets downstream services process at their own pace.

---

## 🧩 Queue vs Pub/Sub — Two Patterns

| Pattern     | Consumer Groups          | Partitions            | Use Case                                    |
| ----------- | ------------------------ | --------------------- | ------------------------------------------- |
| **Queue**   | 1 consumer group         | = number of consumers | Task processing (each message handled once) |
| **Pub/Sub** | Multiple consumer groups | Many                  | Broadcasting (each group gets all messages) |

---

## ⚙️ Advanced Bits (Quick Overview)

### Producer Settings

- **Fire and forget** — send and don't wait (fastest, but risky)
- **Synchronous** — wait for confirmation before continuing
- **Asynchronous** — send with a callback

**Acknowledgment (ACK) levels:**

- `ACK 0` — don't wait at all (fastest, can lose data)
- `ACK 1` — wait for leader to confirm (balanced)
- `ACK All` — wait for all replicas to confirm (safest, slowest)

### Consumer Settings

- **Auto commit** — offset is saved automatically after reading
- **Manual commit** — your code explicitly saves the offset (safer, more control)
- **Partition assignment strategies:** Range, Round Robin, Sticky

### Compression + Batching

Producers can batch multiple messages together and compress them before sending. This improves throughput and reduces disk usage but uses more CPU.

---

## 📝 Quick Reference Cheat Sheet

| Term               | Simple Explanation                                        |
| ------------------ | --------------------------------------------------------- |
| **Message**        | One piece of data (like a GPS ping)                       |
| **Topic**          | Category of messages (like a folder)                      |
| **Partition**      | A chunk of a topic (for scaling)                          |
| **Offset**         | Position of a message in a partition                      |
| **Producer**       | Sends messages to Kafka                                   |
| **Consumer**       | Reads messages from Kafka                                 |
| **Consumer Group** | A team of consumers sharing work                          |
| **Broker**         | A single Kafka server                                     |
| **Cluster**        | Group of brokers working together                         |
| **Replication**    | Copies of partitions on different brokers                 |
| **Rebalancing**    | Kafka redistributing partitions when consumers join/leave |
| **Zookeeper**      | Manages cluster metadata                                  |

---

## 💡 When Should You Use Kafka?

✅ You have a high volume of events (thousands per second)  
✅ Multiple services need to react to the same event  
✅ You need real-time data pipelines  
✅ You need fault tolerance and durability  
✅ You want to decouple your services

❌ Don't use Kafka if you just need simple request-response between two services — that's overkill.

---

> **Used by:** Uber, Netflix, LinkedIn, Spotify, Slack, Pinterest, Coursera
>
> **Originally built at:** LinkedIn | **Currently maintained by:** Confluent
