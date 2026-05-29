# Task Orchestrator
The task orchestrator is a workflow executor built on top of [Bevy ECS](https://bevy.org/learn/quick-start/getting-started/ecs/) and [OpenRMF's Crossflow](https://github.com/open-rmf/crossflow).

## Interfaces
### Task Request
Publishes a task request to instruct an asset to perform a specific task. The message includes the task type, command, task parameters, and expected time window.

Payload:
```json
{
    "type": "TaskRequest",
    "id": "urn:ngsild:Task:task_Depalletize001:TaskRequest",  // Unique identifier, used as reference id for other modules
    "task_type": "Depalletize",  // Type of task, used to distinguish execution methods
    "task_command": "START",  // Task command to START / PAUSE / RESUME / CANCEL a task
    "asset_id": "MANIP1",  // Target asset identifier 
    "task_params": {  // Additional parameters for task
        "area_id":"Outgoing1",
    },
    "timestamp": "2025-01-09T15:30:15Z",  // Timestamp for when task was issued
    "task_expected_start":"2025-01-09T14:30:15",
    "task_expected_end":"2025-01-09T15:30:15",
}
```


### Task Status
Assets publish task status messages to report the outcome or progress of a requested task.

Payload:
```json
{
    "id": "urn:ngsild:Task:task_Depalletize001:TaskStatus"
    "task_type": "Depalletize",
    "status": "RUNNING",
    "asset_id": "",
    "task_params": {},
    "timestamp": "2025-01-09T15:30:15Z",
    "task_expected_start":"2025-01-09T14:30:15",
    "task_expected_end":"2025-01-09T15:30:15",
}
```
### AMQP Consumer
The orchestrator consumes messages from the AMQP `@RECEIVE@` exchange (queue: `@RECEIVE@-rmf_schedule`). Only messages with type set to `Schedule` are processed; other message types (e.g. TaskRequest, TaskStatus) are ignored by the consumer. The fields `type`, `id` and `payload` are required.

```json
{
    "type": "Schedule",
    "id": "workflow-run-001",
    "payload": {
        "version": "0.1.0",
        "start": "node_1",
        "ops": {
            "node_1": {
                "type": "node",
                "builder": "MQTTDeviceReqNode",
                "config": {
                    "asset_id": "MANIP1",
                    "task_id": "Depalletize001",
                    "task_type": "Depalletize",
                    "area_id": "Outgoing1"
                },
                "next": "node_2"
            },
            "node_2": {
                "type": "node",
                "builder": "MQTTDeviceReqNode",
                "config": {
                    "asset_id": "SNS1",
                    "task_id": "Store001",
                    "task_type": "Store",
                    "area_id": "Incoming1"
                },
                "next": { "builtin": "terminate" }
            }
        }
    }
}
```

## Node Types
### GoToNode 
AMQP-based task execution node that publishes `TaskRequest` messages and waits for `TaskStatus` responses.

### MQTTDeviceReqNode
MQTT-based node for coordinating asset operations. Waits for asset `IDLE` status before publishing a task request and waits for a `COMPLETED` task status response.
#### MQTT Topics:
`asset/{asset_id}/asset_status` — Subscribe to check asset is IDLE before sending a request.

`asset/{asset_id}/task_request` — Publish the task request.

`asset/{asset_id}/task_status` — Receive task completion/failure status.

The payload follows the same `TaskRequest` format as the generic interface, with device specific `task_params`
```json
{
    "id": "urn:ngsild:Task:task_Depalletize001:TaskRequest",
    "asset_id": "MANIP1",
    "task_type": "Depalletize",
    "task_command": "START",
    "task_params": {
        "area_id": "Outgoing1"
    },
    "timestamp": "2025-01-09T15:30:15Z",
    "task_expected_start": "2025-01-09T14:30:15",
    "task_expected_end": ""
}
```

## Build and Installation
### Running the TO
To run the TO:

```bash
cargo run
```

The default endpoint for the diagram editor can be found [here](http://localhost:2727/)

