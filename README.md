# RMF2 Task Orchestrator

**RMF2 Task Orchestrator** is a workflow executor for multi-robot task coordination, built on top of [Bevy ECS](https://bevy.org/learn/quick-start/getting-started/ecs/) and [OpenRMF's Crossflow](https://github.com/open-rmf/crossflow).

### Getting Started

```bash
cargo run
```

The diagram editor is available at [http://localhost:2727/](http://localhost:2727/)

### Documentation

- **Platform**: Ubuntu 22.04+

Task Orchestrator

- [Interfaces (TaskRequest, TaskStatus, AMQP Consumer)](./Documentation/interfaces.md)

- [Node Types (AMQP, MQTT, Utility)](./Documentation/nodes.md)

### License

[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)
