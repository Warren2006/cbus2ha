# Reliability validation — 21 October 2026

Candidate: 1.0.5-rc1, branch `reliability/v1.0.4-connection-recovery`, based on
`7dd4d5048a1df4c45c2f945119c3fe231dcfe7f2`. No merge or HA deployment was performed.

## Results

| Check | Result |
| --- | --- |
| Unmodified baseline, Python 3.10.22 / Paho 1.5.0 | 353 tests: 97 passed, 256 pre-existing topic-parser contract errors |
| Final full suite, Python 3.10.22 / Paho 1.5.0 / serial-asyncio 0.4 | **390 tests passed**, including 11 Docker/loopback integration tests; 28.129 s |
| Modern runtime, Python 3.13.5 / Paho 1.6.1 / pyserial 3.5 / serial-asyncio 0.6 | **379 tests passed**, 11 opt-in integration tests skipped; 0.774 s |
| Prepared Mosquitto/CNI service smoke | PASS: 257 discovery messages; ON/OFF -> CNI -> confirmation -> MQTT state; physical frame -> MQTT state; clean SIGTERM |
| CLI help / Python compilation / shell syntax / `git diff --check` | PASS |
| Existing add-on file-presence validator | PASS; this validator does not validate Supervisor runtime semantics |
| Local exact production Dockerfile build | **Blocked** at Alpine package indexes: `dl-cdn.alpinelinux.org` denied by development network policy |
| GitHub Actions, application commit `61ea307` | **All four jobs passed**: Paho 1.5.0 and 1.6.1 unit matrix on Python 3.11, exact Docker build/offline smoke and 11 loopback integration tests |

The successful remote validation is
[run 39546043471](https://github.com/Warren2006/cbus2ha/actions/runs/39546043471).
The image package logs confirm Python 3.11.14, Paho 1.6.1-r2, pyserial 3.5-r6 and
serial-asyncio 0.6. This verifies the candidate build, not the exact production
image currently installed. Subsequent documentation-only commits leave that
tested application code unchanged; check the PR for checks on its latest head.

The failed local image build fetched base
`ghcr.io/home-assistant/amd64-base:3.19@sha256:064e13687f5d2b839c2f7304ae57c71fed4d60f5311e8c8b11ae75fa32c11c6f`,
then could not fetch Alpine main/community indexes. The resulting "no such
package" errors are downstream of that network denial. No TLS or network-policy
bypass was attempted, and the Dockerfile was not changed to work around it.
This is not a completed production image build. Check the GitHub build before
deployment.

The baseline test correction preserves the production parser: bare, state and
config topics remain rejected; valid `/set` topics remain accepted. No production
topic validation was weakened to make tests pass. Existing event tests were
adapted only for the new lifecycle and real subscribe return shape.

## Failure scenarios exercised

| Scenario | Evidence |
| --- | --- |
| Broker disconnect/restart | Real Mosquitto container restart; wait for new connection and SUBACK; all 257 discovery topics return; commands and events work |
| Broker half-open connection | Docker pause preserves TCP sockets while broker stops responding; keepalive detects failure; unpause recovers while C-Bus processing continues |
| Missing initial CONNACK | Start new MQTT client against paused broker; observe handshake timeout/retry, then successful recovery |
| CNI TCP EOF | Close simulated CNI TCP stream; new connection sends the three PCI reset commands and resumes control |
| Silent interface | Stop acknowledging without closing TCP; two failed matched probes cause reopen/init; commands resume |
| PCI reboot | Send duplicate `++` power-up notifications; exactly one reconnect occurs |
| Overlapping failures | Broker restart while CNI stops answering; independent supervisors recover both |
| HA MQTT client restart | Restart the observer/subscriber; event topics have no retained/replayed button event |
| Add-on daemon restart | Stop/start actual `python -m cbus.daemon.cmqttd` subprocesses; commands/events resume; SIGTERM exits zero |
| Diagnostic stack capture | SIGUSR1 on actual daemon subprocess emits thread stacks; INFO/severity logger format and clean shutdown verified |
| Event-loop responsiveness | Deliberately block MQTT reconnect in its network thread while asyncio continues processing bus frames |
| Bounded shutdown | Stalled connect cannot hold the asyncio/default-executor shutdown join indefinitely |
| No synthetic Saturn events | Decode binary and level status replies, reconnect and replay cached state; no event publication |
| Genuine commands | Decode on/off/ramp/terminate frames and preserve exact topic/JSON/QoS/retain; controlled live delivery once per frame |
| Repeated recovery | Five CNI reconnects and three broker restarts preserve live delivery without duplicate events; mock reconnects restore one subscription set per connection |
| Cleanup and resource ownership | Old protocol tasks finish; old transports detach; cancelled connection attempts clean partially created transports; queue startup is idempotent |
| Failure diagnostics | Assert normal-level probe failures, rejected CONNACK/SUBACK, invalid command payloads, publish failure and task-failure logs |
| Idle bus and unrelated traffic | Idle PCI acknowledgements prevent recovery; unsolicited physical traffic cannot satisfy a missing probe confirmation |
| Backoff | Connection-refused injection verifies capped delays and clean per-attempt protocols |
| Time and confirmation ownership | Monotonic command deadlines; probe codes cannot collide with pending commands/system traffic; optional/done loss futures are safe |

## Reproduce

Use a supported Python environment with Paho, six, parameterized and
pyserial/pyserial-asyncio. The prepared legacy environment used Python 3.10
because serial-asyncio 0.4 cannot import on Python 3.11+.

```sh
python -m unittest -v
CBUS_INTEGRATION=1 python -m unittest -v
bash -n run.sh entrypoint-cmqttd.sh validate_addon.sh
git diff --check
```

The opt-in tests require Docker/local sockets, bind broker and CNI only to
loopback, use temporary free ports, and clean only their own containers and
processes. Mosquitto is pinned to
`eclipse-mosquitto@sha256:199ea8ef2e35ec2b1b37e59cfd1dbae538ed4dfa4a2251a121a52215a6248a21`.
No MQTT credentials or physical C-Bus hardware are required. In a sandbox that
disables socket/thread wakeups, allow local socket access for the tests.

No real Home Assistant Supervisor, authenticated/TLS production broker, physical
CNI/PCI/Saturn or months-long soak validation is claimed. See
[operation and remaining risks](reliability-operation.md) for the staged trial,
serial driver caveat, event-loss limits and immediate rollback procedure.
