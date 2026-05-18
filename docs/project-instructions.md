# Project Instructions

## WEDNESDAY: Complete Workflow Phase 1-3

Follow the instructions in
[⭐ **Workflow: Apply Example**](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/).

Complete:

1. Phase 1. **Start & Run** - copy the project and confirm it runs
2. Phase 2. **Change Authorship** - update the project to your name and GitHub account
3. Phase 3. **Read & Understand** - review the project structure and code

## FRIDAY/SUNDAY: Complete Workflow Phases 4-5

Complete:

1. Phase 4. **Make a Technical Modification**
2. Phase 5. **Apply the Skills to a New Problem**

---

## Topic

Streaming messages with Kafka producers and consumers.

This project introduces Kafka as a message broker for streaming data.

The example project:

- produces sales messages to a Kafka topic
- consumes messages from Kafka topic
- processes each raw message consumed
- writes consumed records to a local CSV file
- logs each message as it arrives

## Example Files

Review these files before making changes:

| File | Purpose |
| --- | --- |
| `src/streaming/kafka_producer_case.py` | Produces sales messages to Kafka |
| `src/streaming/kafka_consumer_case.py` | Consumes raw messages from Kafka |
| `src/streaming/core/utils.py` | Provides shared project helpers |

The example data starts in:

```text
data/sales.csv
```

Run commands are in `README.md`.

## Phase 4: Make a Small Technical Modification

Copy the consumer case file:

```text
src/streaming/kafka_consumer_case.py
```

Rename your copy:

```text
src/streaming/kafka_consumer_yourname.py
```

Run your copied file and make one small change.

Good options include:

- change the **KAFKA_TOPIC** name in `.env`
- change the **PRODUCER_MESSAGE_COUNT** in `.env`
- change the **PRODUCER_MESSAGE_INTERVAL_SECONDS** in `.env`
- change the number of messages consumed
- write selected message fields to the output CSV
- change the message printed after each successful consume

Keep your change small enough that you can explain it clearly.

## Optional: Modify the Producer

You can leave the producer unchanged.

To customize the producer:

1. Copy `src/streaming/kafka_producer_case.py`.
2. Rename it `src/streaming/kafka_producer_yourname.py`.
3. Change the message source or message fields.
4. Add your run command to README.md.
5. Run the full pipeline again.

## Phase 5: Apply the Skills

Apply the consumer in a slightly different way.

You may:

- extend the current sales example
- change the Kafka message key
- add a count of messages by the Kafka key
- write selected consumed fields to CSV
- compare how different message keys appear in the stream

Document your work in `docs/index.md`.

Explain:

- what changed from the case example
- what your producer sends
- what your consumer receives
- what you learned from watching messages move through Kafka

## Kafka in the Real World

These examples are professional, but not designed for production.
They have been modified for experimentation,
so we can re-run and re-start frequently and start with
a fresh set of messages.

In production, real messages are produced,
replicated multiple times, and only deleted
after processing has been confirmed.

Streaming systems must be:

- scalable - allow adding more machines as message volume grows
- reliable - avoid losing messages
- durable - keep messages long enough for consumers to process them
- fault tolerant - keep working when one machine or process fails
- observable - provide logs, metrics, and alerts
- secure - control who can produce, consume, and manage topics
- ordered when needed - preserve message order within a key or partition
- replayable when needed - allow consumers to re-read earlier messages
- backpressure-aware - handle producers that send faster than consumers can process
