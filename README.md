# mivi-control-pane

Rust gRPC service that bridges the Go backend and the Holoscan C++/CUDA pipeline. Receives commands over gRPC, forwards them to Holoscan via a custom TCP protocol (port 8557), and publishes pipeline lifecycle events on NATS.

## Architecture

```
mivi-backend (Go)
    │  gRPC  StartExam / StopExam / StartRecording / StopRecording / GetStatus
    ▼
mivi-control-pane (Rust / Tonic)
    │  TCP 8557  MCTR JSON protocol  → commands
    │  UDP 8556  clock-sync protocol ← health RTT
    ▼
mivi-holoscan (C++ / CUDA)
    │  UDP 8554  H.265 stream → Quest 3
    │  MinIO     chunked recording upload
    ▼
NATS  mivi.pipeline.*  mivi.recording.*
    │
    ▼
mivi-backend (NATS subscriber)
    │  finalizes exam when manifest.json is complete
    │  publishes mivi.exam.completed
```

## Session state machine

```
Idle
  │ StartExam (gRPC) + TCP ACK accepted
  ▼
Starting  ←── health watchdog polls every 200 ms
  │ pipeline healthy within START_EXAM_TIMEOUT
  ▼                                         │ timeout / health KO
Active                               Error ──┘
  │ StopExam (gRPC)
  ▼
Stopping
  │ TCP ACK accepted (PipelineStopped)
  ▼
Idle
```

Transitions that produce NATS events:

| Transition | NATS event |
|---|---|
| Idle → Starting (accepted) | `mivi.pipeline.starting` |
| Starting → Active | `mivi.pipeline.started` |
| Starting → Error (timeout) | `mivi.pipeline.start_failed` |
| Active → Stopping | `mivi.pipeline.stopping` |
| Stopping → Idle | `mivi.pipeline.stopped` |
| StartRecording ACK OK | `mivi.recording.started` |
| StopRecording ACK OK | `mivi.recording.stopped` |
| SetSegmentation ACK OK | `mivi.segmentation.updated` |
| Any ACK rejected | `mivi.command.rejected` |
| Health monitor error | `mivi.pipeline.error` |

> `mivi.exam.completed` is **never** published by the control-pane. It is published exclusively by the Go backend after verifying the MinIO manifest.

## NATS event envelope

All events share the same JSON envelope:

```json
{
  "spec_version": "1.0",
  "source": "mivi-control-pane",
  "event_type": "mivi.pipeline.started",
  "exam_id": "...",
  "command_id": "...",
  "ts_ms": 1711234567890,
  "data": {}
}
```

`data` carries event-specific fields (e.g. `"reason"` for `start_failed`/`error`, `"enabled"` for `segmentation.updated`).

## gRPC API

Proto file: `proto/control_pane.proto`

| RPC | Request | Response |
|---|---|---|
| `StartExam` | `exam_id`, `patient_id`, `operator_id`, `AiConfig`, `StorageConfig`, `CaptureConfig` | `CommandResponse` |
| `StopExam` | `exam_id` | `CommandResponse` |
| `StartRecording` | `exam_id` | `CommandResponse` |
| `StopRecording` | `exam_id` | `CommandResponse` |
| `SetSegmentation` | `exam_id`, `enabled` | `CommandResponse` |
| `GetStatus` | `exam_id` | `StatusResponse` |

`CommandResponse` fields: `accepted bool`, `command_id string`, `error_code string`, `error_detail string`.

All RPCs are idempotent for the already-in-target-state case (e.g. StopExam on an already-Idle session returns success immediately).

### StorageConfig

The backend sends:

```json
{ "bucket": "mivi-review", "prefix": "exams/" }
```

Holoscan then writes chunks to `{prefix}{exam_id}/{recording_generation}/chunks/{N:06d}/video.h265` and the manifest to `{prefix}{exam_id}/{recording_generation}/manifest.json`.

## Holoscan TCP protocol (port 8557)

Commands are JSON objects framed with a 4-byte little-endian length prefix.

| `cmd_type` | Description |
|---|---|
| `start_exam` | Start the Holoscan pipeline |
| `stop_exam` | Stop the pipeline and flush MinIO uploads |
| `start_recording` | Begin writing chunks to MinIO |
| `stop_recording` | Stop writing chunks |
| `set_segmentation` | Enable/disable AI segmentation overlay |

Responses: `{ "accepted": true/false, "error_code": "...", "error_detail": "..." }`.

## Health monitoring (UDP 8556)

A background task sends clock-sync requests every `HEALTH_CHECK_INTERVAL_MS` milliseconds. If a response arrives within `COMMAND_TIMEOUT_MS`, the pipeline is considered healthy. The RTT is tracked and exposed via `GetStatus`. A health failure during `Starting` or `Active` transitions the session to `Error` and publishes `mivi.pipeline.error`.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `GRPC_PORT` | `50051` | gRPC listen port |
| `HOLOSCAN_HOST` | `127.0.0.1` | Holoscan hostname / IP |
| `HOLOSCAN_HEALTH_PORT` | `8556` | UDP clock-sync port |
| `HOLOSCAN_COMMAND_PORT` | `8557` | TCP command port |
| `NATS_URL` | `nats://127.0.0.1:4222` | NATS server URL |
| `HEALTH_CHECK_INTERVAL_MS` | `2000` | Health ping interval |
| `START_EXAM_TIMEOUT_MS` | `5000` | Startup health watchdog window |
| `COMMAND_TIMEOUT_MS` | `3000` | TCP command response timeout |
| `LOG_LEVEL` | `info` | tracing filter (e.g. `debug`, `mivi_control_pane=trace`) |

## Building

```bash
cargo build --release
```

Protobuf code is generated at build time via `tonic-build` using the vendored `protoc` binary (`protoc-bin-vendored`). No system `protoc` required.

## Running with Docker Compose

```bash
# From mivi-web-review/
docker compose up --build
```

The control-pane container expects `HOLOSCAN_HOST`, `NATS_URL`, and optionally `GRPC_PORT` to be set via environment variables.

## Key packages

| Module | Role |
|---|---|
| `grpc::handler` | Tonic gRPC service — validates requests, drives session state machine |
| `holoscan::adapter` | TCP command sender + UDP health monitor |
| `holoscan::protocol` | Wire format constants and frame builder/parser |
| `session::registry` | `DashMap`-backed concurrent session store |
| `session::state` | `SessionState` / `SessionEvent` state machine |
| `nats::publisher` | NATS publish helper; best-effort (errors logged, not propagated) |
| `config` | Environment-variable configuration |
