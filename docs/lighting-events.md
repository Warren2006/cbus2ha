# Lighting command events

This optional publisher exposes actual received C-Bus Lighting Application **56
(0x38)** commands independently of the existing Home Assistant light, switch and
binary sensor state publishing. It can be used to control Shelly globes from
C-Bus Saturn switches using MQTT automations. It creates no event discovery
entities and does not change existing topics, discovery identifiers, device
identities, retained state formats, brightness/transition reporting or the
outbound command queue.

## Enable events

Events are **disabled by default**. In the add-on configuration, add the following
field to your existing `mqtt` section, preserving its other settings:

```yaml
mqtt:
  # Keep your existing broker, username, password, TLS and port settings here.
  lighting_events: true
```

Save and restart the bridge during an agreed test window. Missing or `false`
keeps the old behavior. `run.sh` translates this setting into `--lighting-events`.
For a manually launched daemon, append `--lighting-events` to your existing
`cmqttd` command. For `entrypoint-cmqttd.sh`, set `CBUS_LIGHTING_EVENTS=1`;
unset it or set `0` to disable. The Home Assistant Dockerfile uses `run.sh` and
the add-on option, rather than this legacy environment variable.

The publisher covers all valid groups (0–255), including groups without physical
outputs. There is no group or source allowlist; filter in your automation. To
keep a virtual group out of normal discovery/state/subscriptions, append its
address to the existing comma-separated `ga.ignore` setting. Ignored groups
still produce command events when enabled. Without `ignore`, the existing
discovery behavior continues, even for unused groups. Changing `ignore` does
not delete previously retained discovery messages or existing HA entities.

For the examples below, **verify in C-Bus Toolkit that Application 56 group 200
is unused before assigning it to a switch**. Check output units, scenes and
logic too. `ignore` only suppresses bridge entities; it does not prevent other
C-Bus units from reacting. Back up Toolkit configuration before programming.

## Topics and exact payloads

Topic: `cbus/events/lighting/<group_address>` (decimal, no zero padding).
For source unit 46 and group 200, all four examples use
`cbus/events/lighting/200`:

```json
{"event": "on", "source_unit": 46, "group": 200, "application": 56}
```

```json
{"event": "off", "source_unit": 46, "group": 200, "application": 56}
```

```json
{"event": "ramp", "source_unit": 46, "group": 200, "application": 56, "level": 127, "duration": 4}
```

```json
{"event": "terminate_ramp", "source_unit": 46, "group": 200, "application": 56}
```

`source_unit` is the transmitting unit address supplied by the existing decoder;
46 is only an example. The decoder is unchanged and has an existing limitation:
it reports source unit 0 as missing (`None`). Such packets produce no command
events; ordinary state handling retains its existing behavior. This does not
affect source unit 46. `group` is the affected group, not a physical key number.
Only `ramp` contains `level` (decoded target 0–255) and `duration` (decoded C-Bus
ramp duration in seconds). An instantaneous ramp remains a `ramp` with duration
0; a ramp to zero remains a `ramp` with level 0.

The seconds unit is verified against the existing code path:
`LightingRampSAL.decode()` calls `ramp_rate_to_duration()` in `cbus/common.py`,
which maps wire command codes to integer seconds. For example, `0x0a` maps to
4 seconds, `0x3a` to 60 seconds and `0x7a` to 1020 seconds (17 minutes).
`PCIProtocol.handle_cbus_packet()` passes that value to the ramp callback;
the event publisher copies it without scaling or unit conversion. Tests cover
all 16 supported ramp codes through packet decoding to the MQTT payload and
confirm that existing state `transition` uses the same value. This verifies
the software's units, not physical ramp timing on a device.

## Listen in Home Assistant

After deliberately deploying a test version, open **Settings → Devices &
services → MQTT → Configure → Listen to a topic** (labels may vary by HA
version). Subscribe to `cbus/events/lighting/#`, or just
`cbus/events/lighting/200`, and start listening before operating the switch.
Confirm the observed source, group and sequence for press, hold and release.
No old event should appear simply because a listener subscribes: messages are
not retained. Broker ACLs must permit the bridge to publish and HA to subscribe
to `cbus/events/lighting/#`, in addition to the existing discovery/state topics.

The Saturn 5082NL firmware 2.4.00 at unit 46 must be programmed to transmit the
desired commands. The bridge reports commands; it does not infer short/long
presses, releases or key numbers. A release is observable as `terminate_ramp`
only if that is what the switch actually transmits and the SHAC forwards.

## Example MQTT automation

This automation filters the topic, application, source unit and group, then
sets a Shelly light on. Replace `light.shelly_globe` with your actual entity ID.
The topic is a group filter; the JSON checks also restrict the source unit.

```yaml
alias: "Saturn unit 46 group 200 - globe on"
trigger:
  - platform: mqtt
    topic: "cbus/events/lighting/200"
    qos: 1
condition:
  - condition: template
    value_template: >-
      {{ trigger.payload_json.event == 'on'
         and trigger.payload_json.application == 56
         and trigger.payload_json.source_unit == 46
         and trigger.payload_json.group == 200 }}
action:
  - service: light.turn_on
    target:
      entity_id: light.shelly_globe
mode: single
```

For release-to-stop dimming, a second automation can stop a **user-provided**
HA script that repeatedly adjusts the Shelly brightness in small steps:

```yaml
alias: "Saturn unit 46 group 200 - stop dim loop"
trigger:
  - platform: mqtt
    topic: "cbus/events/lighting/200"
    qos: 1
condition:
  - condition: template
    value_template: >-
      {{ trigger.payload_json.event == 'terminate_ramp'
         and trigger.payload_json.application == 56
         and trigger.payload_json.source_unit == 46
         and trigger.payload_json.group == 200 }}
action:
  - service: script.turn_off
    target:
      entity_id: script.shelly_globe_dim_loop
mode: single
```

Create that script and a ramp-triggered start automation separately for your
globes; neither is installed by this feature. Stopping an HA loop prevents its
future steps, but does not cancel a native transition already running on a
Shelly. Use bounded steps and a maximum loop duration so a missed release
cannot leave dimming running indefinitely. A simple OFF automation can reuse
the first example with event `off` and action `light.turn_off`.

## Brightness and delivery limitations

- Terminate Ramp contains no measured brightness. It emits only the event above,
  never a fabricated ON/OFF, brightness or transition update. Existing HA C-Bus
  state may still show the earlier ramp target after termination. No new actual
  brightness tracking is introduced. Use the Shelly entity's own feedback for
  its brightness; C-Bus ramp targets do not measure a Shelly globe's output.
- Ramp values are copied from the decoder. C-Bus uses discrete ramp rates;
  the duration is the protocol value, not a measured elapsed time or a guarantee
  that a Shelly transition has identical behavior.
- Every event uses **QoS 1 and `retain=False`**, allowing at-least-once MQTT
  delivery/retransmission while the client/session can deliver. Duplicates are
  possible. Prefer idempotent actions (`turn_on`/`turn_off`/stop loop) over toggle
  or repeated brightness increments. For ramps, guard against restarting an
  already-running identical loop. Identical consecutive bus commands may also
  be legitimate presses; there is no event ID for perfect deduplication.
- This is not a durable event log. Commands before MQTT is ready, process
  restarts and offline subscribers can lose events. Paho may queue/retransmit
  unacknowledged QoS 1 messages across a connection loss, so an earlier real
  event can arrive late on reconnection. Apply a time limit to dimming loops and
  disable test automations before restarting the bridge.
- Startup, discovery, state publication and reconnection do not manufacture
  events. Sending/queuing an HA command, its confirmation and retries also do
  not manufacture events. If the PCI/SHAC subsequently reports that transmitted
  command as an inbound C-Bus frame, it **will** appear in this stream with the
  reported source unit. Filter the known wall-switch source(s) to avoid loops;
  this stream cannot universally distinguish an observed echo from a press.
- Only Lighting Application 56 on the bridge's existing network/decoder path is
  covered. No hardware or firmware behavior was verified by automated tests.

## Safe testing, deployment and rollback

No deployment is performed by this change. The add-on slug remains `cbus2ha`,
the version remains `1.0.3`, and discovery prefixes and identities are unchanged.
Existing upstream links in `repository.yaml` and software attribution are
preserved; they are metadata, not a redirect of Git pushes. The feature is on
`feature/cbus-lighting-events` in `Warren2006/cbus2ha`; upstream installation
links in the README still install `wazza-aus/cbus2ha`.

Home Assistant Supervisor identifies a repository add-on using both its
repository identity and slug. Adding `https://github.com/Warren2006/cbus2ha`
can produce a separate store entry/install alongside the upstream one, despite
the identical slug/name. A local add-on also has a different identity. Inspect
the repository URL and full add-on identifier before selecting an install or
update. Do not assume this fork transparently updates the upstream add-on.
The add-on store normally uses the repository default branch: this unmerged
feature branch will not automatically become an update by adding the fork URL.

1. Review the branch and run the offline tests with a compatible environment
   (the pinned serial dependency requires Python 3.10 or older):
   `python -m unittest tests.test_lighting_events -v`, then `python -m unittest`.
   No hardware or broker is required. Compare full-suite failures to the baseline.
2. Before any HA trial, take a full HA backup and save the working add-on's
   version/image or source revision, configuration and Toolkit project. Record
   current entity IDs and MQTT discovery settings. Choose the test build route
   explicitly: a controlled local build of this branch, or a reviewed release
   from your fork. A future release needs a deliberate version bump; none is
   made here to avoid an accidental upgrade.
3. Stop the working bridge and disable its watchdog/start-on-boot before starting
   a replacement. Keep its installation for rollback. Ensure only **one** bridge
   can connect to the SHAC and production MQTT broker. Do not test by running
   upstream and fork simultaneously: they share discovery/state identities and
   command subscriptions, causing conflicting states and duplicate commands.
4. Use the same group classifications, labels and existing MQTT settings for a
   deliberate replacement. Do not change discovery prefixes, slug or device IDs
   to test this feature. A truly parallel laboratory test must use simulated
   C-Bus and isolated, non-production MQTT/HA, not the running SHAC or broker.
   Review retained discovery before reconnecting: adding `ignore` prevents new
   publication, but does not remove old retained configs. Never clear all
   discovery topics; any targeted cleanup requires a separate deliberate action.
5. First run with events disabled and verify normal lights, switches, sensors,
   dimming and queued commands. Then enable events, verify group 200 is unused
   in Toolkit, listen to events and check unit 46 ramp/release sequences before
   enabling globe automations. Keep a timeout on any dim loop.
6. Roll back by disabling the new automations and any running dim loop, setting
   `mqtt.lighting_events: false` and restarting the sole bridge. To revert code,
   stop the test bridge and disable its auto-start/watchdog, then restore/start
   the saved working build/configuration. Restore the earlier Toolkit project if
   programming changed. Never restart the original while the test bridge runs.
   Event topics need no retained cleanup because events are never retained.

No changes are made to `cbus/protocol/packet.py` or the existing ramp decoder.
Their behavior, including the source-unit-0 limitation above, is preserved.
