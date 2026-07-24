# RMF2 Task Orchestrator

**RMF2 Task Orchestrator** is a workflow executor for multi-robot task coordination, built on top of [Bevy ECS](https://bevy.org/learn/quick-start/getting-started/ecs/) and [OpenRMF's Crossflow](https://github.com/open-rmf/crossflow).

### Getting Started

**Prerequisites:** MQTT broker (eg. [Mosquitto](https://mosquitto.org/)) and [RabbitMQ](https://www.rabbitmq.com/) running locally.

```bash
cargo run
```

**Run an example workflow:**

```bash
curl -X POST http://localhost:2727/api/executor/run \
  -H 'Content-Type: application/json' \
  -d '{"diagram": '"$(cat diagrams/mqtt_examples/mqtt_listen_consume.json)"', "request": {}}'
```

Alternatively, the workflow can be edited and run on the live editor at <http://localhost:2727>.

![Diagram Editor](docs/images/diagram_editor_example.png)


### Documentation

- **Platform**: Ubuntu 22.04+
- **Protocol Support**: MQTT, AMQP

Detailed Documentation

- [Task Interfaces](./docs/interfaces.md)

- [Node Types](./docs/nodes.md)

### License

[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)
