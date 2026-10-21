# Reliability candidate: operation, diagnostics and rollback

The test candidate is **1.0.5-rc1**, based on production **1.0.4** at
`7dd4d5048a1df4c45c2f945119c3fe231dcfe7f2`. Read
[the source comparison](reliability-investigation.md) before deployment.
Nothing in this change deploys to Home Assistant or merges master.

## Changed behavior

MQTT uses one Paho network thread for I/O, keepalive and retries with a 1–60 s
backoff. Its callbacks marshal work to asyncio; MQTT disconnect/reconnect cannot
block C-Bus command processing. Accepted CONNACK restores subscriptions and
discovery. SUBACK refusal, publish failure and failed connection attempts are
visible at normal logging levels. MQTT packets and credentials are not logged
by the new network diagnostics.

C-Bus has one connection supervisor. Each attempt is limited to 30 s, retries
back off from 1 to 60 s, and a fresh handler/transport is used each time. Backoff
resets after a connection survives 60 s. Connection loss cancels and joins
queue, confirmation, probe and timesync tasks. Queued/unconfirmed commands are
discarded with a warning instead of executing unexpectedly after a reconnect.
MQTT remains active while C-Bus reconnects and rejects commands during that gap.

`cbus.link_check_interval` defaults to 60 s; zero disables probes. A probe is a
read-only level status request for groups 0–31 with a uniquely reserved PCI
confirmation code. It is not a lighting command or a full-network state scan.
Only the matching successful confirmation satisfies it, not noise or unrelated
switch activity. A quiet, functioning bus answers the PCI request. Two failed
probes, each with a 5 s response window, reopen and initialise the interface.
Worst-case default detection is about 130 s plus reconnect time; MQTT detection
uses the default 60 s keepalive and Paho's ping timeout, rather than a separate
daemon watchdog. A PCI power-up notification also requests reconnection;
duplicate notifications are coalesced.

All existing MQTT entity IDs, topics, payloads, QoS and retained discovery/state
behavior remain. Genuine App 56 commands still generate the existing optional
nonretained Saturn events. Status replies never enter that command-event path.
MQTT recovery republishes only the latest cached **observed** states, including
switches and binary sensors, without creating command events. It cannot infer
state changes missed while the C-Bus link was disconnected.

The C-Bus command queue is bounded at 256 and Paho's outgoing queue at 1024.
During MQTT disconnection, state updates coalesce by topic and live events are
counted as unavailable, rather than building an unbounded historical button
event queue. On restoration the loss count is logged. These physical presses
cannot be delivered live during an outage. Genuine QoS 1 packets already sent
but unacknowledged can still be redelivered by MQTT; exactly-once HA automation
execution is not guaranteed. No synthetic events are created to fill gaps.

The CLI configures logging explicitly because `cbus/common.py` installs a
handler at import time. Previously this could make later `basicConfig`, including
`--log-file`, ineffective. Normal logs include time, severity and logger. A
five-minute heartbeat records MQTT and C-Bus state, subscription readiness,
last RX/probe-ack age and command queue counts. Unbuffered Python startup and
explicit SIGTERM/SIGINT cleanup improve log visibility and shutdown behavior.
Unexpected network-driver/supervisor termination produces a nonzero exit rather
than leaving a running, ineffective bridge. Connection failures themselves are
repaired without restarting the daemon.

## Before any installation

1. Keep the production add-on installed, its current image cached, and its
   current v1.0.4 options available. Do not upgrade or uninstall that copy.
   Make a Home Assistant backup including the add-on and securely export its
   options. Those options contain MQTT credentials; do not paste them into logs,
   issue comments or shell command arguments.
2. Record the production add-on slug, architecture, version and actual image
   ID/digest. A source archive alone cannot preserve the exact installed runtime.
   If host Docker access is available, export that image to a safe backup with
   `docker image save <production-image-id> -o <safe-backup-path>`.
3. Preserve the baseline Git commit above (also held on the rollback branch).
   Its source archive is available from GitHub:
   `https://github.com/Warren2006/cbus2ha/archive/7dd4d5048a1df4c45c2f945119c3fe231dcfe7f2.tar.gz`.
4. Require the PR's Docker build and regression checks to pass before installing.
   Local Docker package downloads may be restricted by the development network;
   local Python/loopback success is not proof of a completed production image.

## Safe test installation

Use a local add-on on a test Home Assistant instance first. To build the candidate
without changing the original repository installation:

1. Check out the exact reviewed commit from
   `reliability/v1.0.4-connection-recovery`. Copy its source to a new local add-on
   folder, for example `/addons/cbus2ha-reliability`, using the supported host
   file-access method. Do not copy `.git`, development virtualenvs or test logs.
   Keep the supplied slug and schemas; the Home Assistant local repository
   supplies a separate namespace. Avoid a conflicting existing local slug.
2. Reload local add-ons in the Add-on Store, build/install the **1.0.5-rc1** local
   candidate and copy the saved settings securely in the UI. Leave lighting
   events at their existing setting. Leave `link_check_interval: 60` initially.
3. At a planned cutover, stop production and temporarily disable its start-on-boot
   and watchdog options. Only then start the candidate. Never run both against
   the same CNI/PCI or MQTT entity topics: CNIs can limit TCP clients and both
   bridges would duplicate state/events. Keep the candidate manual-start during
   early testing so a reboot cannot unexpectedly start two bridges.
4. Check accepted MQTT connection, successful subscription restoration and C-Bus
   connection logs. Confirm normal on/off, dimming, ramps, all configured device
   types and physical Saturn press/hold/release automations. Confirm actual
   switch events appear once per genuine observed command and retain=false.
5. In a maintenance window, exercise a broker restart, candidate stop/start,
   Home Assistant MQTT integration reload, and a CNI restart. Check recovery logs
   and ensure reconnection produces no button events. Do not power-cycle a live
   automation interface without planning for its impact.
6. Observe overnight and then for several weeks before treating the candidate
   as proven unattended operation. Enable its start-on-boot only after confirming
   production remains disabled. Keep the original installed for rollback.

An alternative is to use the same exact commit in an existing local build
workflow. Preserve the same stop-before-start rule and the original image and
settings; no change here instructs automatic HA deployment.

## Immediate rollback

Stop **1.0.5-rc1**, disable its automatic startup, then start the retained
production **1.0.4** add-on with its original options. Restore the original
start-on-boot preference. Since discovery identifiers, topics and payloads are
unchanged, entity re-creation should not be necessary. Observe normal light
control and a physical Saturn command before leaving the system unattended.
If installation used an in-place local build, restore the saved production image
and v1.0.4 source/options through that workflow; do not depend on rebuilding a
moving master branch during an outage.

## Capture during the next failure, before restarting

* Export the add-on log, broker log and HA MQTT integration log covering at
  least the preceding 15 minutes. Record local time and timezone, what action
  failed, whether physical switches still control lights, and whether other
  MQTT integrations are working.
* Check the add-on's actual running state and uptime, last five-minute heartbeat,
  disconnect/retry messages, subscription result, probe failures and command
  confirmation timeouts. A missing heartbeat while the process is alive suggests
  a stalled loop/logging path, rather than proving a broker fault. A rising
  RX age on its own does not prove failure: bus inactivity is legitimate.
* With host Docker access, inspect only status/image fields, for example:
  `docker inspect --format '{{.State.Status}} {{.State.StartedAt}} {{.State.ExitCode}} {{.Image}}' <addon-container>`.
  Avoid dumping full inspect output, environment, options or auth files.
* Observe a real physical press in an authenticated MQTT viewer on
  `cbus/events/lighting/<group>` and the corresponding state topic. Compare with
  daemon/broker logs. Avoid logging passwords in CLI arguments or shell history.
  Do not publish lighting commands merely to diagnose a disconnected live system.
* The candidate registers **SIGUSR1 on the Python daemon** for all-thread stack
  dumps to stderr. Inside the container, identify its exact process using
  `ps`/`pgrep -af cbus.daemon.cmqttd`, verify the PID, and signal that PID with
  `kill -USR1 <python-pid>`. Capture the resulting add-on log. **Do not send
  SIGUSR1 to the container's `/init` or to the old v1.0.4 process**, which has no
  such handler. Thread stacks show locations, not local variables or passwords.

## Limits and unverified assumptions

No physical Saturn, CNI, PCI, USB unplug/replug, firmware, actual HA Supervisor
or months-long soak has been tested. The loopback CNI acknowledges valid requests
as expected from the protocol; real PCI confirmation behavior and serial driver
behavior must be confirmed during the staged trial. Disable probes with zero if
an interface proves incompatible, then capture diagnostics before deciding on a
different probe/recovery method.

There is an inherited low-level serial limitation: pyserial-asyncio 0.4 and 0.6
call `serial.flush()` synchronously when closing a transport. A kernel/USB driver
stuck inside flush/close can still stall the event loop; an asyncio timeout cannot
interrupt that system call. This patch does not introduce a custom serial driver
or claim to fix a wedged USB host controller. Serial open/close and real USB fault
recovery remain unverified; TCP failures are exercised with real sockets.

MQTT DNS/TCP/TLS attempts run off asyncio, but a broken system DNS resolver can
still delay an individual retry. The shutdown join has a five-second limit so
it cannot strand the asyncio executor waiting on that resolver. Paho's network
driver hook is verified for 1.5.0/1.6.1; future Paho changes need compatibility
testing. TLS/authentication outages and broker ACL rejection are diagnosed but
no real authenticated production broker or client certificate was tested.

The existing 250 ms command confirmation timeout and three retries are preserved;
unusually slow/loaded hardware may still produce retries. Unexpected binary or
unsupported frames drop the current receive buffer with an error log, allowing
subsequent commands to decode. Frames in that discarded buffer may be lost.
No software change can deliver physical presses missed while either link is down.
