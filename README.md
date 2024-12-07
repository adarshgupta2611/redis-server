# Redis Clone Project

This project is a simplified clone of Redis, a popular in-memory data structure store, used as a database, cache, and message broker. The project implements various Redis functionalities, including handling streams, keys, and replication.

## Features

- **Stream Operations**: 
  - `XADD`: Appends a new entry to a stream with optional auto-generated timestamps and sequence numbers.
  - `XRANGE`: Retrieves a range of entries from a stream based on specified start and end IDs.
  - `XREAD`: Reads a range of items from streams, supporting blocking operations to wait for new entries.

- **Key Operations**:
  - `GET`: Retrieves the value associated with a given key.
  - `SET`: Sets the value of a key.
  - `TYPE`: Returns the type of value associated with a key.

- **Replication**:
  - Handles replication configurations and waits for a specified number of replicas to acknowledge write operations.

- **Configuration and Info**:
  - `CONFIG`: Retrieves configuration details.
  - `INFO`: Provides information about the server.

- **Protocol Conversion**:
  - Converts strings and stream entries to RESP (Redis Serialization Protocol) format for communication.

## Architecture

- **Main Server**: Listens for client connections and handles incoming commands concurrently.
- **Command Helpers**: Functions to process specific commands and perform necessary operations.
- **Utilities**: Helper functions for common tasks such as parsing arguments and converting data formats.

## Getting Started

### Prerequisites

- Python 3.x

### Running the Server

To run the server locally, execute the following command:

```bash
./your_program.sh
```

This script sets up the server on the default port (6379) and starts listening for client connections.

### Connecting to the Server

You can use a Redis client or a simple socket connection to interact with this server. Ensure your client is configured to connect to `localhost` on port `6379`.

## License

This project is licensed under the MIT License.