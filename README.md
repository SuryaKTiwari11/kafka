# Kafka & Microservices Setup

This repository contains a simple Apache Kafka setup using Docker, along with a Node.js Producer and Consumer to demonstrate real-time message streaming.

## 🚀 Prerequisites

Make sure you have the following installed on your machine:
- [Docker & Docker Compose](https://www.docker.com/)
- [Node.js](https://nodejs.org/)

## 🛠️ Setup Instructions

### 1. Start the Kafka & Zookeeper Containers

Run the following command in your terminal to start the containers in the background:

```bash
docker-compose up -d
```

### 2. Install Dependencies

Install the required Node.js packages (specifically `kafkajs`):

```bash
npm install
```

### 3. Initialize the Kafka Topic

Before sending messages, we need to create the Kafka topic `rider-updates`. Run the admin script to set this up:

```bash
node admin.js
```
*You should see a success message indicating the topic was created.*

---

## 🏃‍♂️ Running the Application

To see the real-time message streaming in action, you'll need two separate terminal windows.

### Terminal 1: Start the Consumer

The consumer will listen for any incoming messages on the `rider-updates` topic.

```bash
node consumer.js my-group
```
*(Replace `my-group` with any consumer group name you'd like).*

### Terminal 2: Start the Producer

The producer allows you to interactively type messages and send them to Kafka.

```bash
node producer.js
```

Once it connects and you see the `> ` prompt, try typing some inputs in this format: `<RiderName> <Location>`

**Examples:**
```text
> Rider1 North
> Rider2 South
```

If you look at your **Consumer terminal**, you will instantly see the messages being received and processed from different partitions!

## 🛑 Stopping the Services

When you're done testing, you can tear down the Docker containers to free up resources:

```bash
docker-compose down
```
