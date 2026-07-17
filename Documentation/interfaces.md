# Interfaces

## Task Request
Publishes a task request to instruct an asset to perform a specific task. The message includes the task type, command, task parameters, and expected time window.

Payload:
```json
{
    "type": "TaskRequest",
    "id": "urn:ngsild:Task:task_Depalletize001:TaskRequest",
    "task_type": "Depalletize",
    "task_command": "START",
    "asset_id": "MANIP1",
    "task_params": {
        "area_id":"Outgoing1"
    },
    "timestamp": "2025-01-09T15:30:15Z",
    "task_expected_start":"2025-01-09T14:30:15",
    "task_expected_end":"2025-01-09T15:30:15"
}
```

## Task Status
Assets publish task status messages to report the outcome or progress of a requested task.

Payload:
```json
{
    "id": "urn:ngsild:Task:task_Depalletize001:TaskStatus",
    "task_type": "Depalletize",
    "status": "RUNNING",
    "asset_id": "",
    "task_params": {},
    "timestamp": "2025-01-09T15:30:15Z",
    "task_expected_start":"2025-01-09T14:30:15",
    "task_expected_end":"2025-01-09T15:30:15"
}
```

## AMQP Consumer
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
