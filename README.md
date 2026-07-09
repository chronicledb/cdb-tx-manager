# cdb-tx-manager

The transaction manager service for CDB. Exposes a gRPC API that clients use to read items, look up the current sequence number, and commit transactions. Sits between clients and the underlying chronicle (append-only log) and storage engine (materialized view), enforcing write-schema validation and sequence-number-based conflict detection on every commit.

## Responsibilities

- **Serve reads** (`getItems`) directly against a fast, queryable storage engine (Redis).
- **Serve the current sequence number** (`getSeqNum`) so clients can start a transaction against a known baseline.
- **Validate and commit transactions** (`commitTransaction`):
  - Validates every operation's table and attributes against the configured write schema.
  - Forwards valid transactions to the chronicle service for durable, ordered append.
  - Returns success/failure — including sequence-number conflicts — back to the client.

This service does not itself resolve conflicts or retry; it enforces schema validity up front and relies on the chronicle service's sequence-number check to reject transactions that raced with a conflicting commit.

## Architecture

```
Client
  │  gRPC (getItems / getSeqNum / commitTransaction)
  ▼
TxManagerServiceImpl
  │                              │
  │ getItems / getSeqNum         │ commitTransaction
  ▼                              ▼
StorageEngine (Redis)     WriteSchema validation ──▶ ChronicleServiceClient ──▶ Chronicle service
```

- **`TxManagerServiceImpl`** — the gRPC service implementation; orchestrates validation, storage reads, and chronicle appends.
- **`StorageEngine`** — interface for reads and the current sequence number, backed by `RedisStorageEngine`. Reads a table/key's current value and the global sequence number atomically via a Redis transaction (`MULTI`/`EXEC`).
- **`WriteSchema`** — parsed once at startup from a configured JSON string. Defines allowed tables, their attributes, primary key(s), and other constraints. Every `commitTransaction` operation is validated against it before being forwarded to the chronicle.
- **`ChronicleServiceClient`** — gRPC client to the separate chronicle service, which owns durable, sequenced storage of committed transactions.
- **`Transaction` / `Operation`** — internal representations of a client's transaction and its individual PUT/DELETE operations, serialized to JSON when sent to the chronicle.

## Configuration

Configured via Spring properties (e.g. `application.properties`/`application.yml` or environment variables):

| Property | Description |
|---|---|
| `cdb.chronicle.service.ip` | Host of the chronicle service this instance talks to |
| `cdb.chronicle.service.port` | Port of the chronicle service |
| `chronicle-id` | ID of the chronicle this tx-manager instance is associated with |
| `write-schema-json` | The write schema (JSON) this instance validates transactions against |

Redis connection settings are configured via standard Spring Data Redis properties (e.g. `spring.data.redis.host`, `spring.data.redis.port`).

> **Note:** the gRPC channel to the chronicle service is currently configured with `usePlaintext()` (see `ChronicleServiceClient`) — TLS is not yet configured and should be added before production use.

## gRPC API

| RPC | Description |
|---|---|
| `getItems(GetItemsRequest)` | Look up one or more items by table + primary key. Returns each result along with the current sequence number, so callers can detect if their view is stale. |
| `getSeqNum(Empty)` | Returns the current sequence number, used by clients to initialize a transaction. |
| `commitTransaction(CommitTransactionRequest)` | Validates and commits a batch of operations. Fails with a descriptive error if any operation violates the write schema, or if the chronicle rejects the transaction (e.g. due to a sequence number mismatch). |

## Write schema validation

On commit, each operation's table must exist in the configured `WriteSchema`, and every attribute in the operation's data must be a recognized attribute for that table. Unknown tables or attributes cause the entire transaction to fail with `FAILURE` and a descriptive `errorMessage`, without ever reaching the chronicle.