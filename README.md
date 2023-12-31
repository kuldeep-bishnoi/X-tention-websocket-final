# WebSocket Application

This repository contains a WebSocket application built in Go (Golang) that serves as a real-time data server using the Gin framework and interacts with Redis for message broadcasting. It connects to a WebSocket service provided by a third-party API and handles various WebSocket operations.

## Features

- WebSocket connections with client rate limiting.
- Subscription and broadcasting of real-time data.
- Integration with Redis for message distribution.
- Support for subscribing to instrument tokens.
- Updating API token and key dynamically.

## Prerequisites

Before running this application, ensure you have the following:

- Go (Golang) installed on your system.
- Redis server running, or you can use a cloud-based Redis service.
- Access to a WebSocket API (e.g., Kite Trade API) for real-time data.
- Environment variables set in a `.env` file:
  - REDIS_ADDR: Address of the Redis server.
  - REDIS_DB: Redis database number.
  - SERVER_ADDRESS: Address where the application will run.
  - TOKEN_SECRET: Secret key for PASETO token verification.
