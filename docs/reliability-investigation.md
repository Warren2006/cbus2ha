# C-Bus reliability comparison — 21 October 2026

This comparison was written before implementing fixes. It establishes defects,
not the cause of the particular production failure: no packet capture, process
dump, broker log, installed image digest or live hardware was supplied.

## Revisions and actual historical image

| Source | Exact revision examined |
| --- | --- |
| Warren2006/cbus2ha, installed v1.0.4 | `7dd4d5048a1df4c45c2f945119c3fe231dcfe7f2` |
| abnormaliti/cbus2ha | `2d5b1e21bd811af5f78e0cbada22ed271e3a844e` |
| wazza-aus/cbus2ha, current default master | `a6ba5ee2bb64b8ae2f11d6e5c38c28669da4f27f` |
| MtSamsonite/ha-cbus2mqtt, main | `e2905ab8235e8fd3c351f291573350a8f60971ba` |
| MtSamsonite/cbusha, candidate source, master | `fb389b6221f924dc06fc4641103b7a95863749c4` |
| Nik19792000/cbus, micolous/cbus PR 31 head | `e99b46755bc1910836364eec441380a3a633c9dc` |

GitHub source links use these revisions, not moving branches. Upstream master
and main point to the same commit; the upstream beta branch was listed but is
not represented as reviewed or recommended here.

The historical wrapper's `cbus2mqtt/config.yaml` selects
`mtsamsonite/cbus2mqtt-{arch}:0.6.0`; its Dockerfile only inherits that image and
runs `/entrypoint-cmqttd.sh`. The README credits micolous/cbus and explicitly
includes PR 31. Examining the wrapper alone would miss the implementation.

The amd64 image was downloaded and inspected with networking disabled:

* `mtsamsonite/cbus2mqtt-amd64@sha256:249aadc3d54b0e8ce484e475c376d55955bb1253c757bfdb44141d4921972f96`
* Python 3.10.4, cbus distribution 0.2, Paho 1.6.1, pyserial 3.5,
  pyserial-asyncio 0.6; Alpine 3.16 base labels.
* Its labels surprisingly say add-on 0.5.0 although the selected tag is 0.6.0.
* Extracted `cmqttd.py` Git blob hash:
  `81efb44d4741cf6e38c3f98437ee55d53a4768e0`.
* Extracted `pciprotocol.py` Git blob hash:
  `35a0a50d4d47ad28e7b61f0f84dcf5e0623a5524`.
* Both files exactly match PR 31's head above. `paho_asyncio.py` is blob
  `e7cc71926fefe3c9503c7d7f382c71a55d457584`, also identical to v1.0.4.

Thus the actual image's core source is identified more precisely than its
package version. The current MtSamsonite/cbusha master is a different,
multi-application implementation and must not be assumed to be the image source.
This inspected amd64 image is not proof of the architecture, tag or immutable
digest used in the former installation. Other architectures remain uninspected.

### Runtime verification after implementation

The unchanged production Dockerfile was successfully built by
[GitHub Actions run 39546043471](https://github.com/Warren2006/cbus2ha/actions/runs/39546043471)
for application commit `61ea3079b1face33a6461c8d910174ff42d7f0ca`. Its package
installation logs show Python **3.11.14**, Paho **1.6.1-r2**, pyserial **3.5-r6**
and pip-installed pyserial-asyncio **0.6**, on the configured Alpine 3.19 base.
Those dependency versions were built from the same Dockerfile as v1.0.4;
they are not evidence of the exact image currently installed on the user's host.

Production, upstream and Abnormaliti have identical `Dockerfile`,
`requirements-cmqttd.txt`, `requirements.txt` and `setup.py` Git blobs.
The requirements files' Paho 1.5.0/serial-asyncio 0.4 pins do **not** describe
that image build: the Dockerfile installs APK packages plus serial-asyncio 0.6,
and setup.py uses `requires`, not pip's `install_requires`. Thus the historical
image and the newly verified build both use Paho 1.6.1 and serial-asyncio 0.6.
Blaming a nominal Paho 1.5-versus-1.6 downgrade on the basis of the requirements
file would be unsupported. Python/base-image versions and application behavior
differ; the installed production runtime still needs its own digest/version
capture. The local legacy test environment deliberately exercises Paho 1.5.0 too.

## Architectural comparison and source evidence

All four relevant daemons use asyncio PCIProtocol and Paho MQTT. Production,
upstream, Abnormaliti and the inspected historical image use the same
`cbus/paho_asyncio.py:AsyncioHelper`: socket readers/writers plus a one-second
`loop_misc()` task. They do not run Paho's `loop_start()`/`loop_forever()`, which
are the paths providing automatic MQTT reconnection.

| Area | Production v1.0.4 / upstream | Historical image | Abnormaliti |
| --- | --- | --- | --- |
| MQTT connect | Synchronous `connect()` in `_main`; no disconnect callback/retry | Same | Synchronous `reconnect()` inside async retry loop; 1–60 s backoff |
| Half-open MQTT | `loop_misc()` drives keepalive, then stops when socket is lost; no recovery | Same | Disconnect schedules recovery, but an attempt blocks asyncio |
| Subscriptions | `on_connect` subscribes and republishes discovery, ignores CONNACK rc | Same underlying pattern | Checks rc and resubscribes |
| CNI/PCI socket loss | Resolves one future; `_main` returns, no reopen | Same | Up to 30 initial attempts, then exit on established socket loss |
| Silent PCI | No probe or reinitialisation | No probe | Probe after RX inactivity; mark down and repeat resets on existing port |
| Polling | No startup status poll | PR 31 binary poll after 5 s; state-only MQTT callbacks | Level polls at startup/recovery; status converted into lighting callbacks |
| Commands | 100 ms queue, 250 ms confirmation timeout, 3 retries | Direct sends, no production confirmation queue | Keeps production queue |
| Events | Opt-in inbound App 56 events, QoS 1, nonretained | No custom Saturn event topics | No custom Saturn event implementation |
| Lifecycle | HA script `exec python3 -m...` under base `/init`; no connection healthcheck | Base `/init` runs shell wrapper/daemon; no connection supervisor | Signals and graceful stop added |

Specific production evidence:

* `cmqttd._main()` calls `mqtt_client.connect()` on asyncio, then waits only for
  `connection_lost_future`. A broker loss does not resolve the C-Bus future.
* `AsyncioHelper.misc_loop()` exits on a non-success `loop_misc()` return;
  `on_socket_close()` removes readers and cancels misc. Neither restarts a link.
* `PCIProtocol.connection_lost()` unconditionally sets a possibly absent/done
  future, clears the transport and has no reconnect owner.
* `PCIProtocol.connection_made()` creates an untracked timesync task. After
  loss it can later raise `IOError('transport not connected')`.
* `CBusHandler.connection_lost()` cancels queue/watchdog; only MQTT
  `MqttClient.on_connect()` starts them. Reopening C-Bus independently without
  repairing this ownership would leave commands stuck until MQTT also reconnects.
* Queue startup checks `_queue_running`, but sets it inside the task, leaving
  a window for duplicate queue/watchdog tasks. Confirmation handlers are also
  untracked. Timeout arithmetic uses wall time, which can jump under NTP.
* `MqttClient.publish()` and plain state/event publication do not inspect return
  codes. Paho's default unbounded QoS queue can grow during an outage.
* `CBusHandler` emits Saturn events only from inbound lighting callbacks, with
  a source address; HA commands/confirmations use state-only MQTT methods.
  Retaining that distinction is essential.
* `BufferedProtocol._process_buffer()` leaves a problematic frame in the buffer
  if decoding/callback processing raises. Repeated input can repeat the error
  until overflow clears the buffer. This is a secondary diagnostic/recovery risk.
* `cbus/common.py` calls `logging.basicConfig` at import time. The daemon's later
  `basicConfig(filename=...)` can therefore be ignored. Explicit logger levels
  still permit INFO records; this does not prove that logging caused the incident.
  Application disconnect callbacks and heartbeat diagnostics are missing.

## Ranked explanations for the reported failure

1. **MQTT loss followed by no reconnection — high confidence defect; leading
   hypothesis for a running but quiet add-on.** Keepalive can detect a lost broker
   or half-open connection, but detection is not reconnection. Without commands
   arriving, queue logs also become quiet. Restart creates a new MQTT session.
   No disconnect callback means no useful application transition log.
2. **Open CNI socket but stopped PCI communications — medium confidence for the
   incident, high confidence recovery is absent.** A reboot or network blackhole
   can leave no EOF and no traffic. Normal lighting inactivity is indistinguishable
   from failure without an active, acknowledged probe. A fresh connection and
   PCI reset on restart can recover it.
3. **CNI EOF/transport error — high confidence defect, lower fit if container was
   still running.** `_main` returns normally; the current HA entrypoint replaces
   itself with Python. Both the inspected historical image and the current base
   use an S6 `/init` entrypoint, so that `exec` does not make Python container
   PID 1. `boot: auto` is not an application health monitor, and
   there is no configured add-on watchdog. Container status at failure would
   distinguish this from hypothesis 1.
4. **Callback/parser/background task failure or queue stall — plausible, not
   demonstrated by production evidence.** Parser exceptions, unchecked payload
   fields and orphaned tasks can interrupt work; diagnostic coverage is weak.
   Queue timing/command bursts and dependencies differ from the old image.
5. **Home Assistant MQTT integration/state or host problem — unresolved.** A HA
   restart may restore subscriptions/integration, also restart the broker or
   reset other components. That observation does not localise the fault.

These are defect confidence rankings, not measured incident probabilities. The
same fundamental reconnect gaps exist in the historical image, so the code does
not prove why it failed only monthly. Different network conditions, Paho/Python
versions, command rates and HA lifecycle remain possible factors.

## Abnormaliti assessment

`MqttClient.connect_with_retry()` calls `reconnect()` synchronously at startup.
`_reconnect_loop()` awaits backoff but then also calls synchronous `reconnect()`.
DNS, TCP establishment and TLS handshakes can stall all C-Bus processing and
confirmation deadlines. Successful return only means CONNECT was sent, not
successful CONNACK; returning the retry task at that point leaves rejection and
no-CONNACK cases dependent on subsequent callbacks. Attempts repeat indefinitely
with capped backoff, which is appropriate for an unattended system, but the
blocking attempt and handshake ownership are not safe to copy.

Its `_link_monitor()` distinguishes idle traffic by sending a probe, logs missed
responses and repeats `pci_reset()` without reopening a troublesome USB port.
Those diagnostics and the recovery intent are useful. However any received
bytes satisfy a probe, even noise/unrelated traffic; no confirmation matching is
required. It cannot reopen a broken established TCP connection. Queue/watchdog,
timesync and initial-sync tasks are not all joined during teardown. Repeated
resets on a dead serial driver can keep queuing output. This is not evidence
of a proven leak on every recovery, but cleanup/retry ownership is incomplete.

`PCIProtocol._handle_extended_cal()` converts binary/level reports into
`on_lighting_group_on/off/ramp(source_addr, ...)`. Startup and recovery invoke
`request_level_status()`. Combining those paths with v1.0.4's event-emitting
handler would create false physical Saturn events, with a real reporting unit
address. Testing only `source_addr is None` would not prevent it. Status reports
also need an explicit lighting-application check before interpreting levels.
Do not copy that implementation into the production event path.

## Selected fixes and exclusions

* Let Paho's single network thread own MQTT I/O, keepalive and capped automatic
  retries; marshal all application callbacks to asyncio. Do not combine that
  thread with AsyncioHelper. Restore subscriptions/discovery and cached state on
  accepted CONNACK; validate payloads and log failed publication.
* Use one asyncio C-Bus connection supervisor with bounded individual attempts,
  capped retry delay, fresh protocol objects and cleanup of owned tasks. MQTT
  must remain independent during C-Bus recovery. Disconnected commands must not
  accumulate and execute unexpectedly after recovery.
* Match active PCI probes to confirmation codes, tolerate a missed probe, then
  reconnect/reinitialise the interface. This tests idle links without assuming
  silence is a failure and requires no physical lighting command.
* Keep status replies outside the Saturn event path. No wholesale status decoder,
  startup full-network scan, discovery schema changes or entity changes are
  necessary for these fixes. MQTT resync uses only already observed states;
  it does not claim to know changes missed while C-Bus was disconnected.
* Track/cancel/join tasks, use monotonic command deadlines, provide normal-level
  connection diagnostics and a periodic health summary. Handle shutdown signals.
* Bound MQTT buffering. Offline button events cannot be delivered live and must
  not be manufactured from cached state. Preserve existing topics, JSON, QoS and
  retain behavior for live events. MQTT QoS 1 can redeliver genuine unacknowledged
  events; it cannot promise end-to-end exactly-once automation execution.

Useful historical contribution: verified PR 31 keeps state reporting distinct
from command events; its runtime versions expose why requirements.txt alone
does not identify a deployed image. Useful upstream contribution: the device
types/command confirmation behavior already inherited by v1.0.4. Current default
upstream contains no additional reliability patch beyond the installed baseline.

## Baseline validation

Before changes, Python 3.10.22 with Paho 1.5.0 ran **353 tests: 97 passed,
256 errors**. Every error is the existing `tests/test_cmqttd.py` contract mismatch:
the test passes a bare/state/config topic to a parser which now accepts only
command `/set` topics. Preserve production topic validation and correct the
outdated test expectations rather than weakening the parser. Hardware and HA
Supervisor behavior have not been validated.

## Pinned source references

* Production [daemon and queue](https://github.com/Warren2006/cbus2ha/blob/7dd4d5048a1df4c45c2f945119c3fe231dcfe7f2/cbus/daemon/cmqttd.py),
  [MQTT adapter](https://github.com/Warren2006/cbus2ha/blob/7dd4d5048a1df4c45c2f945119c3fe231dcfe7f2/cbus/paho_asyncio.py),
  [PCI transport](https://github.com/Warren2006/cbus2ha/blob/7dd4d5048a1df4c45c2f945119c3fe231dcfe7f2/cbus/protocol/pciprotocol.py),
  [buffer processing](https://github.com/Warren2006/cbus2ha/blob/7dd4d5048a1df4c45c2f945119c3fe231dcfe7f2/cbus/protocol/buffered_protocol.py).
* Historical [wrapper/image selection](https://github.com/MtSamsonite/ha-cbus2mqtt/blob/e2905ab8235e8fd3c351f291573350a8f60971ba/cbus2mqtt/config.yaml),
  [PR 31 daemon matching the image](https://github.com/Nik19792000/cbus/blob/e99b46755bc1910836364eec441380a3a633c9dc/cbus/daemon/cmqttd.py),
  [PR 31 PCI matching the image](https://github.com/Nik19792000/cbus/blob/e99b46755bc1910836364eec441380a3a633c9dc/cbus/protocol/pciprotocol.py).
* Current, different [MtSamsonite candidate source](https://github.com/MtSamsonite/cbusha/tree/fb389b6221f924dc06fc4641103b7a95863749c4).
* Abnormaliti [reconnect/link monitor](https://github.com/abnormaliti/cbus2ha/blob/2d5b1e21bd811af5f78e0cbada22ed271e3a844e/cbus/daemon/cmqttd.py),
  [status-to-lighting conversion](https://github.com/abnormaliti/cbus2ha/blob/2d5b1e21bd811af5f78e0cbada22ed271e3a844e/cbus/protocol/pciprotocol.py).
* Current default [upstream revision](https://github.com/wazza-aus/cbus2ha/tree/a6ba5ee2bb64b8ae2f11d6e5c38c28669da4f27f).

Implementation, tests, deployment and residual limitations are documented in
[reliability-operation.md](reliability-operation.md) and
[reliability-validation.md](reliability-validation.md).
