#!/usr/bin/env python3
# cmqttd.py - MQTT connector for C-Bus
# Copyright 2019-2020 Michael Farrell <micolous+git@gmail.com>
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

import asyncio
from asyncio import (
    get_event_loop, run, Queue, QueueEmpty, QueueFull, create_task, sleep)
from argparse import ArgumentParser, FileType
from collections import deque
from dataclasses import dataclass, field
import faulthandler
import json
import logging
import signal
import threading
import time
from typing import Any, BinaryIO, Dict, Optional, Text, TextIO

import paho.mqtt.client as mqtt

try:
    from serial_asyncio import create_serial_connection
except ImportError:
    async def create_serial_connection(*_, **__):
        raise ImportError('Serial device support requires pyserial-asyncio')

from cbus.common import (
    MIN_GROUP_ADDR, MAX_GROUP_ADDR, check_ga, Application, CONFIRMATION_CODES)
from cbus.daemon.recovery import maintain_cbus_connection, log_health
from cbus.protocol.pciprotocol import PCIProtocol
from cbus.protocol.application.status_request import StatusRequestSAL
from cbus.protocol.pm_packet import PointToMultipointPacket
from cbus.toolkit.cbz import CBZ


logger = logging.getLogger(__name__)

_BINSENSOR_TOPIC_PREFIX = 'homeassistant/binary_sensor/cbus_'
_LIGHT_TOPIC_PREFIX = 'homeassistant/light/cbus_'
_SWITCH_TOPIC_PREFIX = 'homeassistant/switch/cbus_'
_TOPIC_SET_SUFFIX = '/set'
_TOPIC_CONF_SUFFIX = '/config'
_TOPIC_STATE_SUFFIX = '/state'
_META_TOPIC = 'homeassistant/binary_sensor/cbus_cmqttd'
_LIGHTING_EVENT_TOPIC_PREFIX = 'cbus/events/lighting/'

# Device type constants
_DEVICE_TYPE_LIGHT = 'light'
_DEVICE_TYPE_LIGHT_NON_DIMMABLE = 'light_non_dimmable'
_DEVICE_TYPE_SWITCH = 'switch'
_DEVICE_TYPE_BINARY_SENSOR = 'binary_sensor'
_DEVICE_TYPE_IGNORE = 'ignore'


def ga_range():
    return range(MIN_GROUP_ADDR, MAX_GROUP_ADDR + 1)


def get_topic_group_address(topic: Text) -> int:
    """Gets the group address for the given topic."""
    # Support multiple topic prefixes
    prefixes = [
        _LIGHT_TOPIC_PREFIX,
        _SWITCH_TOPIC_PREFIX,
        _BINSENSOR_TOPIC_PREFIX,
    ]
    
    for prefix in prefixes:
        if topic.startswith(prefix) and topic.endswith(_TOPIC_SET_SUFFIX):
            ga = int(topic[len(prefix):].split('/', maxsplit=1)[0])
            check_ga(ga)
            return ga
    
    raise ValueError(
        f'Invalid topic {topic}, must start with a known prefix and end with {_TOPIC_SET_SUFFIX}')


def set_topic(group_addr: int) -> Text:
    """Gets the Set topic for a group address."""
    return _LIGHT_TOPIC_PREFIX + str(group_addr) + _TOPIC_SET_SUFFIX


def state_topic(group_addr: int) -> Text:
    """Gets the State topic for a group address."""
    return _LIGHT_TOPIC_PREFIX + str(group_addr) + _TOPIC_STATE_SUFFIX


def conf_topic(group_addr: int) -> Text:
    """Gets the Config topic for a group address."""
    return _LIGHT_TOPIC_PREFIX + str(group_addr) + _TOPIC_CONF_SUFFIX


def bin_sensor_state_topic(group_addr: int) -> Text:
    """Gets the Binary Sensor State topic for a group address."""
    return _BINSENSOR_TOPIC_PREFIX + str(group_addr) + _TOPIC_STATE_SUFFIX


def bin_sensor_conf_topic(group_addr: int) -> Text:
    """Gets the Binary Sensor Config topic for a group address."""
    return _BINSENSOR_TOPIC_PREFIX + str(group_addr) + _TOPIC_CONF_SUFFIX


def get_device_type(group_addr: int, device_types: Dict[int, str]) -> str:
    """Get device type for a group address, defaulting to dimmable light."""
    return device_types.get(group_addr, _DEVICE_TYPE_LIGHT)


def conf_topic_for_device(group_addr: int, device_type: str) -> Text:
    """Get config topic based on device type."""
    if device_type == _DEVICE_TYPE_SWITCH:
        return _SWITCH_TOPIC_PREFIX + str(group_addr) + _TOPIC_CONF_SUFFIX
    elif device_type == _DEVICE_TYPE_BINARY_SENSOR:
        return _BINSENSOR_TOPIC_PREFIX + str(group_addr) + _TOPIC_CONF_SUFFIX
    else:  # light or light_non_dimmable (both use light topic)
        return conf_topic(group_addr)


def set_topic_for_device(group_addr: int, device_type: str) -> Text:
    """Get set topic based on device type (command topic).
    
    Note: For simplicity, all device types use the light set topic for commands.
    """
    return set_topic(group_addr)


def state_topic_for_device(group_addr: int, device_type: str) -> Text:
    """Get state topic based on device type."""
    if device_type == _DEVICE_TYPE_SWITCH:
        return _SWITCH_TOPIC_PREFIX + str(group_addr) + _TOPIC_STATE_SUFFIX
    elif device_type == _DEVICE_TYPE_BINARY_SENSOR:
        return bin_sensor_state_topic(group_addr)
    else:  # light or light_non_dimmable (both use light topic)
        return state_topic(group_addr)


@dataclass
class QueuedCommand:
    """Represents a command waiting to be sent or verified"""
    command_type: str  # 'on', 'off', 'ramp'
    group_addr: int
    device_type: str
    params: dict = field(default_factory=dict)  # brightness, transition_time, etc.
    confirmation_code: Optional[bytes] = None
    timestamp: float = 0  # When command was sent
    retry_count: int = 0
    max_retries: int = 3
    is_retry: bool = False  # True if this is a retry (for priority)
    mqtt_state_update: Optional[dict] = None  # State to publish to HA after success


class CBusHandler(PCIProtocol):
    """
    Glue to wire events from the PCI onto MQTT
    """
    mqtt_api = None

    def __init__(self, labels: Optional[Dict[int, Text]], 
                 device_types: Optional[Dict[int, str]] = None, 
                 *args, lighting_events: bool = False,
                 link_check_interval: float = 60,
                 link_probe_timeout: float = 5, **kwargs):
        super().__init__(*args, **kwargs)
        self.labels = (
            labels if labels is not None else {})  # type: Dict[int, Text]
        self.device_types = (
            device_types if device_types is not None else {})  # type: Dict[int, str]
        self.lighting_events = lighting_events
        
        # Queue system
        self.command_queue = Queue(maxsize=256)
        self.retry_queue = deque()  # Priority queue for retries (FIFO within retries)
        self.pending_confirmations = {}  # confirmation_code -> QueuedCommand
        self.queue_processor_task = None
        self.timeout_watchdog_task = None
        self.queue_lock = None  # Will be initialized as asyncio.Lock when loop is available
        self._queue_running = False
        self._tasks = set()
        self._connection_started = False
        self._recovering = False
        self.link_check_interval = link_check_interval
        self.link_probe_timeout = link_probe_timeout
        self._probe_code = None
        self._probe_future = None
        self.last_rx_time = None
        self.last_probe_time = None

    def _start_task(self, coroutine, name):
        task = create_task(coroutine, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._task_finished)
        return task

    def _task_finished(self, task):
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error('C-Bus background task %s failed', task.get_name(),
                         exc_info=task.exception())
            self.request_recovery('background task failure')

    def connection_made(self, transport):
        self._connection_started = True
        try:
            super().connection_made(transport)
        except Exception:
            logger.exception('C-Bus PCI initialisation failed')
            self.request_recovery('PCI initialisation failed')
            return
        # Queue lifetime belongs to the C-Bus link, independently of MQTT.
        self.start_queue_system()
        if self.link_check_interval > 0:
            self._start_task(self._link_monitor(), 'cbus-link-monitor')

    def data_received(self, data):
        self.last_rx_time = time.monotonic()
        try:
            super().data_received(data)
        except Exception:
            # A bad/unsupported frame must not remain at the head of the buffer
            # and prevent every later physical switch command from decoding.
            self._buf.clear()
            logger.exception('C-Bus input processing failed; dropped buffered input')

    def request_recovery(self, reason):
        if self._recovering or self._transport is None:
            return
        self._recovering = True
        logger.warning('C-Bus recovery requested: %s', reason)
        self._transport.abort()

    def on_pci_power_up(self):
        self.request_recovery('PCI power-up notification; reinitialisation required')

    def _get_confirmation_code(self):
        # A five-second probe can overlap enough commands to wrap the code
        # alphabet. Never reuse its code or a pending lighting command's code.
        for _ in CONFIRMATION_CODES:
            code = super()._get_confirmation_code()
            if code != self._probe_code and code not in self.pending_confirmations:
                return code
        raise RuntimeError('All PCI confirmation codes are in use')

    async def _link_monitor(self):
        failures = 0
        while True:
            await sleep(self.link_check_interval)
            self._probe_future = get_event_loop().create_future()
            try:
                # Read-only request: never send a lighting on/off/ramp as a
                # health probe. Its status replies do not enter lighting events.
                self._probe_code = self._send(PointToMultipointPacket(
                    sals=StatusRequestSAL(
                        child_application=Application.LIGHTING,
                        level_request=True, group_address=0)))
                answered = await asyncio.wait_for(
                    self._probe_future, self.link_probe_timeout)
            except (OSError, asyncio.TimeoutError):
                answered = False
            finally:
                if self._probe_future is not None and not self._probe_future.done():
                    self._probe_future.cancel()
                self._probe_code = None
                self._probe_future = None
            if answered:
                self.last_probe_time = time.monotonic()
                if failures:
                    logger.info('C-Bus PCI probe responding again')
                failures = 0
            else:
                failures += 1
                logger.warning('C-Bus PCI probe unanswered or rejected (%s/2)', failures)
                if failures >= 2:
                    self.request_recovery('two failed PCI probes')
                    return

    async def shutdown(self):
        self.stop_queue_system()
        tasks = set(self._tasks)
        if self._timesync_task is not None:
            tasks.add(self._timesync_task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


    def _publish_lighting_event(self, event, source_addr, group_addr, **kwargs):
        # Only inbound callbacks use this path, never queued HA commands or
        # confirmation-driven state updates. A missing source is not a bus event.
        if not self.lighting_events or not self.mqtt_api or source_addr is None:
            return
        try:
            self.mqtt_api.publish_lighting_event(
                event, source_addr, group_addr, **kwargs)
        except Exception:
            # An optional event failure must not interrupt normal state handling.
            logger.exception('Could not publish lighting command event')

    def on_lighting_group_ramp(self, source_addr, group_addr, duration, level):
        if not self.mqtt_api:
            return
        self._publish_lighting_event(
            'ramp', source_addr, group_addr, duration=duration, level=level)
        device_type = get_device_type(group_addr, self.device_types)
        
        # Log but don't publish for ignored devices
        if device_type == _DEVICE_TYPE_IGNORE:
            logger.info(f"Received CBUS ramp event for ignored device GA {group_addr}, ignoring")
            return
        
        self.mqtt_api.lighting_group_ramp(
            source_addr, group_addr, duration, level, device_type)

    def on_lighting_group_on(self, source_addr, group_addr):
        if not self.mqtt_api:
            return
        self._publish_lighting_event('on', source_addr, group_addr)
        device_type = get_device_type(group_addr, self.device_types)
        
        # Log but don't publish for ignored devices
        if device_type == _DEVICE_TYPE_IGNORE:
            logger.info(f"Received CBUS on event for ignored device GA {group_addr}, ignoring")
            return
        
        self.mqtt_api.lighting_group_on(source_addr, group_addr, device_type)

    def on_lighting_group_off(self, source_addr, group_addr):
        if not self.mqtt_api:
            return
        self._publish_lighting_event('off', source_addr, group_addr)
        device_type = get_device_type(group_addr, self.device_types)
        
        # Log but don't publish for ignored devices
        if device_type == _DEVICE_TYPE_IGNORE:
            logger.info(f"Received CBUS off event for ignored device GA {group_addr}, ignoring")
            return
        
        self.mqtt_api.lighting_group_off(source_addr, group_addr, device_type)

    def on_lighting_group_terminate_ramp(self, source_addr, group_addr):
        # This command contains no brightness measurement. Leave HA state alone.
        self._publish_lighting_event('terminate_ramp', source_addr, group_addr)

    def on_clock_request(self, source_addr):
        self.clock_datetime()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """Called when CBUS connection is lost - stop queue system."""
        logger.warning('C-Bus connection lost (%s)',
                       type(exc).__name__ if exc is not None else 'EOF/closed')
        self.stop_queue_system()
        for task in self._tasks:
            task.cancel()
        dropped = (self.command_queue.qsize() + len(self.retry_queue)
                   + len(self.pending_confirmations))
        if dropped:
            logger.warning('C-Bus disconnected; discarded %s unconfirmed/queued commands', dropped)
        self.pending_confirmations.clear()
        self.retry_queue.clear()
        while not self.command_queue.empty():
            self.command_queue.get_nowait()
        super().connection_lost(exc)

    # Queue system methods
    
    async def queue_command(self, command_type: str, group_addr: int,
                           device_type: str, params: dict, mqtt_state: dict):
        """
        Enqueue a command for processing.
        
        :param command_type: 'on', 'off', or 'ramp'
        :param group_addr: Group address
        :param device_type: Device type string
        :param params: Command parameters (brightness, duration, etc.)
        :param mqtt_state: State to publish to HA after successful confirmation
        """
        cmd = QueuedCommand(
            command_type=command_type,
            group_addr=group_addr,
            device_type=device_type,
            params=params,
            mqtt_state_update=mqtt_state,
            timestamp=0,
            retry_count=0,
            max_retries=3,
            is_retry=False
        )
        
        if self._recovering or (self._connection_started and self._transport is None):
            logger.warning('C-Bus disconnected; rejected command for GA %s', group_addr)
            return
        try:
            self.command_queue.put_nowait(cmd)
        except QueueFull:
            logger.warning('C-Bus command queue full; rejected command for GA %s', group_addr)
            return
        logger.debug(f"Queued command: GA {group_addr} {command_type}")

    def _send_queued_command(self, cmd: QueuedCommand) -> Optional[bytes]:
        """
        Sends a queued command to CBUS and returns the confirmation code.
        Uses the transport's non-blocking write; confirmations arrive later.
        """
        try:
            if cmd.command_type == 'on':
                conf_code = self.lighting_group_on(cmd.group_addr)
                return conf_code
            elif cmd.command_type == 'off':
                conf_code = self.lighting_group_off(cmd.group_addr)
                return conf_code
            elif cmd.command_type == 'ramp':
                conf_code = self.lighting_group_ramp(
                    cmd.group_addr, cmd.params['duration'], cmd.params['level'])
                return conf_code
        except Exception as e:
            logger.error(f"Error sending command GA {cmd.group_addr} {cmd.command_type}: {e}", exc_info=e)
            return None

    async def _queue_processor(self):
        """
        Processes commands from the queue with 100ms rate limiting.
        Prioritizes retries over new commands.
        """
        logger.info("Queue processor started")
        
        # Ensure lock is initialized
        if self.queue_lock is None:
            from asyncio import Lock
            self.queue_lock = Lock()
        
        while self._queue_running:
            try:
                # Check retry queue first (priority)
                if self.retry_queue:
                    cmd = self.retry_queue.popleft()
                    logger.info(f"Processing retry {cmd.retry_count}/{cmd.max_retries} for GA {cmd.group_addr}: {cmd.command_type}")
                else:
                    # Get next command from main queue (non-blocking)
                    try:
                        cmd = self.command_queue.get_nowait()
                    except QueueEmpty:
                        # No commands, wait a bit before checking again
                        await sleep(0.1)
                        continue
                
                # Send command to CBUS (synchronous call)
                confirmation_code = self._send_queued_command(cmd)
                
                if confirmation_code:
                    # Track for confirmation matching
                    async with self.queue_lock:
                        cmd.confirmation_code = confirmation_code
                        cmd.timestamp = time.monotonic()
                        self.pending_confirmations[confirmation_code] = cmd
                    
                    logger.debug(f"Sent command GA {cmd.group_addr} {cmd.command_type}, waiting for confirmation {confirmation_code!r}")
                else:
                    # No confirmation requested or error
                    logger.warning(f"Command GA {cmd.group_addr} {cmd.command_type} returned no confirmation code")
                    # Treat as immediate failure, retry if possible
                    if cmd.retry_count < cmd.max_retries:
                        cmd.retry_count += 1
                        cmd.is_retry = True
                        self.retry_queue.append(cmd)
                    else:
                        logger.error(f"Command GA {cmd.group_addr} {cmd.command_type} failed after {cmd.max_retries} attempts (no confirmation)")
                
                # Wait 100ms before next command (rate limiting)
                await sleep(0.1)
                
            except Exception as e:
                logger.error(f"Queue processor error: {e}", exc_info=e)
                await sleep(0.1)
        
        logger.info("Queue processor stopped")

    async def _timeout_watchdog(self):
        """
        Monitors pending confirmations and retries commands that timeout.
        Runs every 50ms to check for 250ms timeouts.
        """
        logger.info("Timeout watchdog started")
        
        if self.queue_lock is None:
            from asyncio import Lock
            self.queue_lock = Lock()
        
        while self._queue_running:
            await sleep(0.05)  # Check every 50ms
            
            current_time = time.monotonic()
            timed_out = []
            
            async with self.queue_lock:
                for conf_code, cmd in list(self.pending_confirmations.items()):
                    # If no confirmation received within 250ms
                    if current_time - cmd.timestamp > 0.25:
                        timed_out.append((conf_code, cmd))
            
            # Handle timeouts outside the lock
            for conf_code, cmd in timed_out:
                logger.info(f"Confirmation timeout for GA {cmd.group_addr} {cmd.command_type} (code {conf_code!r})")
                
                async with self.queue_lock:
                    if conf_code in self.pending_confirmations:
                        del self.pending_confirmations[conf_code]
                
                # Retry if possible
                if cmd.retry_count < cmd.max_retries:
                    cmd.retry_count += 1
                    cmd.is_retry = True
                    cmd.confirmation_code = None
                    cmd.timestamp = 0
                    self.retry_queue.append(cmd)
                    logger.info(f"Scheduling timeout retry {cmd.retry_count}/{cmd.max_retries} for GA {cmd.group_addr}")
                else:
                    logger.error(f"Command GA {cmd.group_addr} {cmd.command_type} timed out after {cmd.max_retries} retries")
                    # Don't update Home Assistant state
        
        logger.info("Timeout watchdog stopped")

    def start_queue_system(self):
        """Start the queue processor and timeout watchdog."""
        if self._queue_running:
            return
        self._queue_running = True
        
        # Initialize queue lock
        from asyncio import Lock
        if self.queue_lock is None:
            self.queue_lock = Lock()
        
        self.queue_processor_task = self._start_task(self._queue_processor(), 'cbus-commands')
        self.timeout_watchdog_task = self._start_task(self._timeout_watchdog(), 'cbus-confirmations')
        logger.info("Queue system started")

    def stop_queue_system(self):
        """Stop the queue processor and timeout watchdog."""
        self._queue_running = False
        if self.queue_processor_task:
            self.queue_processor_task.cancel()
        if self.timeout_watchdog_task:
            self.timeout_watchdog_task.cancel()
        logger.info("Queue system stopped")

    def on_confirmation(self, code: bytes, success: bool):
        """
        Handle PCI confirmation responses - matches codes to queued commands.
        """
        # Call parent first (for logging)
        super().on_confirmation(code, success)
        if code == self._probe_code and self._probe_future is not None:
            if not self._probe_future.done():
                self._probe_future.set_result(success)
            return
        
        # Match confirmation to pending command
        async def handle_confirmation():
            if self.queue_lock is None:
                from asyncio import Lock
                self.queue_lock = Lock()
            
            async with self.queue_lock:
                if code not in self.pending_confirmations:
                    # This is expected for system commands (time sync, etc.)
                    # that aren't tracked in the queue
                    logger.debug(f"Received confirmation {code!r} with no matching pending command (likely system command)")
                    return
                
                cmd = self.pending_confirmations[code]
                del self.pending_confirmations[code]
            
            if success:
                # Command succeeded - update Home Assistant state
                logger.info(f"Command confirmed: GA {cmd.group_addr} {cmd.command_type} (confirmation {code!r})")
                
                if self.mqtt_api and cmd.mqtt_state_update:
                    # Update Home Assistant state
                    if cmd.command_type == 'on':
                        self.mqtt_api.lighting_group_on(
                            None, cmd.group_addr, cmd.device_type)
                    elif cmd.command_type == 'off':
                        self.mqtt_api.lighting_group_off(
                            None, cmd.group_addr, cmd.device_type)
                    elif cmd.command_type == 'ramp':
                        self.mqtt_api.lighting_group_ramp(
                            None, cmd.group_addr,
                            cmd.params['duration'], cmd.params['level'],
                            cmd.device_type)
            else:
                # Command failed - retry if possible
                logger.info(f"Command failed: GA {cmd.group_addr} {cmd.command_type} (confirmation {code!r})")
                
                if cmd.retry_count < cmd.max_retries:
                    cmd.retry_count += 1
                    cmd.is_retry = True
                    cmd.confirmation_code = None
                    cmd.timestamp = 0
                    # Add to retry queue (priority)
                    self.retry_queue.append(cmd)
                    logger.info(f"Scheduling retry {cmd.retry_count}/{cmd.max_retries} for GA {cmd.group_addr}")
                else:
                    logger.error(f"Command GA {cmd.group_addr} {cmd.command_type} failed after {cmd.max_retries} retries")
                    # Don't update Home Assistant state - it stays as-is
        
        self._start_task(handle_confirmation(), 'cbus-confirmation-callback')


class MqttClient(mqtt.Client):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bridge_handler = kwargs.get('userdata')
        self._config_handler = self.bridge_handler
        self._asyncio_loop = None
        self._stopping = False
        self._state_cache = {}
        self._publish_failed = False
        self._offline_events = 0
        self.network_failure = None
        self.subscriptions_ready = False
        self.max_queued_messages_set(1024)

    def start(self, loop, host, port, keepalive):
        self._asyncio_loop = loop
        self.network_failure = loop.create_future()
        self.reconnect_delay_set(min_delay=1, max_delay=60)
        self.connect_async(host, port, keepalive)
        logger.info('Starting MQTT connection; automatic retry backoff 1–60 seconds')
        # This is the only MQTT network driver. Never install AsyncioHelper
        # alongside loop_start(). DNS/TCP/TLS and retries run off asyncio.
        self.loop_start()

    def attach_handler(self, handler):
        self.bridge_handler = handler
        if handler is not None:
            handler.mqtt_api = self
            self._config_handler = handler

    def _dispatch(self, callback, *args):
        if self._stopping:
            return
        if self._asyncio_loop is None:
            self._invoke(callback, args)
        elif not self._asyncio_loop.is_closed():
            self._asyncio_loop.call_soon_threadsafe(self._invoke, callback, args)

    def _invoke(self, callback, args):
        if self._stopping:
            return
        try:
            callback(*args)
        except Exception:
            # User payload/callback failures must never terminate Paho's thread.
            logger.exception('MQTT application callback %s failed',
                             getattr(callback, '__name__', type(callback).__name__))

    def _thread_main(self):
        try:
            super()._thread_main()
        except Exception:
            logger.exception('MQTT network thread failed')
        finally:
            if not self._stopping:
                self._dispatch(self._network_stopped)

    def _network_stopped(self):
        if self.network_failure is not None and not self.network_failure.done():
            self.network_failure.set_result(True)

    async def stop(self, timeout=5):
        self._stopping = True
        self.disconnect()
        # loop_stop joins a network thread. A stalled system DNS resolver must
        # not trap asyncio or its default executor during process shutdown.
        stopper = threading.Thread(target=self.loop_stop, daemon=True,
                                   name='mqtt-shutdown')
        stopper.start()
        deadline = time.monotonic() + timeout
        while stopper.is_alive() and time.monotonic() < deadline:
            await sleep(0.01)
        if stopper.is_alive():
            logger.warning('MQTT network thread still finishing connection I/O; shutdown wait expired')

    def on_log(self, client, userdata, level, message):
        # Paho 1.5 reports failed attempts only at DEBUG. Promote this specific
        # transition without enabling packet/payload/credential logging.
        if message == 'Connection failed, retrying':
            self._dispatch(logger.warning, 'MQTT connection attempt failed; retrying automatically')

    def on_disconnect(self, client, userdata, rc, properties=None):
        self._dispatch(self._handle_disconnect, rc)

    def _handle_disconnect(self, rc):
        self.subscriptions_ready = False
        logger.warning('MQTT disconnected (rc=%s); retrying automatically', rc)

    def on_subscribe(self, client, userdata, mid, granted_qos, properties=None):
        self._dispatch(self._handle_subscribe, mid, granted_qos)

    def _handle_subscribe(self, mid, granted_qos):
        self.subscriptions_ready = False
        if any(qos >= 128 for qos in granted_qos):
            logger.error('MQTT broker rejected command subscriptions (mid=%s)', mid)
        else:
            self.subscriptions_ready = True
            logger.info('MQTT command subscriptions restored')

    def on_connect(self, client, userdata: CBusHandler, flags, rc):
        self._dispatch(self._handle_connect, userdata, flags, rc)

    def _handle_connect(self, userdata, flags, rc):
        self.subscriptions_ready = False
        if rc != mqtt.CONNACK_ACCEPTED:
            logger.warning('MQTT connection refused (rc=%s); retrying automatically', rc)
            return
        logger.info('Connected to MQTT broker')
        self._publish_failed = False
        userdata = self._config_handler if self._asyncio_loop is not None else userdata
        if userdata is None:
            return
        userdata.mqtt_api = self
        
        # Subscribe to set topics for all device types (except ignored and binary sensors)
        topics = []
        for ga in ga_range():
            device_type = get_device_type(ga, userdata.device_types)
            
            # Skip ignored devices (no subscriptions)
            if device_type == _DEVICE_TYPE_IGNORE:
                continue
            
            # Skip binary sensors (read-only, no commands)
            if device_type == _DEVICE_TYPE_BINARY_SENSOR:
                continue
            
            # Subscribe to light set topic (used for all lights)
            topics.append((set_topic(ga), 2))
            # Also subscribe to switch topics (Home Assistant might send commands to these)
            topics.append((_SWITCH_TOPIC_PREFIX + str(ga) + _TOPIC_SET_SUFFIX, 2))
        
        if topics:
            rc, _ = self.subscribe(topics)
            if rc != mqtt.MQTT_ERR_SUCCESS:
                logger.warning('MQTT subscription restoration failed (rc=%s)', rc)
        else:
            self.subscriptions_ready = True
        self.publish_all_lights(userdata.labels, userdata.device_types)
        for topic, payload in self._state_cache.copy().items():
            self._publish_raw(topic, payload)
        if self._offline_events:
            logger.warning('MQTT restored; %s live lighting events were unavailable during outage',
                           self._offline_events)
            self._offline_events = 0

    def on_message(self, client, userdata: CBusHandler, msg: mqtt.MQTTMessage):
        self._dispatch(self._handle_message, userdata, msg)

    def _handle_message(self, userdata, msg):
        """Handle a message from an MQTT subscription."""
        if self._asyncio_loop is not None:
            userdata = self.bridge_handler
            if userdata is None or userdata._transport is None or userdata._recovering:
                logger.warning('C-Bus disconnected; rejected MQTT command')
                return
        # Check if it's a set topic (any device type)
        if not msg.topic.endswith(_TOPIC_SET_SUFFIX):
            return

        try:
            ga = get_topic_group_address(msg.topic)
        except ValueError:
            # Invalid group address
            logging.error(f'Invalid group address in topic {msg.topic}')
            return

        # Get device type for this group address
        device_type = get_device_type(ga, userdata.device_types)
        
        # Reject commands for ignored devices
        if device_type == _DEVICE_TYPE_IGNORE:
            logger.info(f"Received command for ignored device GA {ga}, ignoring")
            return
        
        # Reject commands for binary sensors (read-only)
        if device_type == _DEVICE_TYPE_BINARY_SENSOR:
            logger.info(f"Received command for read-only binary sensor GA {ga}, ignoring")
            return

        # https://www.home-assistant.io/integrations/light.mqtt/#json-schema
        # Home Assistant sends JSON for lights, but plain strings ("ON"/"OFF") for switches
        try:
            payload = json.loads(msg.payload)
        except (json.JSONDecodeError, ValueError):
            # If JSON parsing fails, treat as plain string (for switches)
            # Home Assistant sends "ON" or "OFF" as plain strings for switches
            try:
                payload_str = msg.payload.decode('utf-8').strip().upper()
                if payload_str in ('ON', 'OFF'):
                    payload = {'state': payload_str}
                else:
                    logging.error(f'Invalid payload format in {msg.topic}: {msg.payload}')
                    return
            except (AttributeError, UnicodeDecodeError):
                # If payload is already a string or decode fails, try direct conversion
                payload_str = str(msg.payload).strip().upper()
                if payload_str in ('ON', 'OFF', '"ON"', '"OFF"'):
                    # Remove quotes if present
                    payload_str = payload_str.strip('"\'')
                    payload = {'state': payload_str}
                else:
                    logging.error(f'Invalid payload format in {msg.topic}: {msg.payload}')
                    return
        except Exception as e:
            logging.error(f'Unexpected error parsing payload in {msg.topic}: {e}', exc_info=e)
            return

        try:
            if not isinstance(payload, dict) or payload.get('state', '').upper() not in ('ON', 'OFF'):
                raise ValueError('state must be ON or OFF')
            light_on = payload['state'].upper() == 'ON'
            brightness = int(payload.get('brightness', 255))
            transition_time = int(payload.get('transition', 0))
        except (ValueError, TypeError, AttributeError, OverflowError):
            logger.warning('Invalid MQTT lighting command on %s', msg.topic)
            return

        # Clamp brightness for non-dimmable lights and switches
        if device_type in (_DEVICE_TYPE_LIGHT_NON_DIMMABLE, _DEVICE_TYPE_SWITCH):
            # Only full on or off
            brightness = 255 if light_on else 0
        else:
            # Normal brightness clamping for dimmable lights
            if brightness < 0:
                brightness = 0
            if brightness > 255:
                brightness = 255

        if transition_time < 0:
            transition_time = 0

        # For non-dimmable lights and switches, ignore transition
        if device_type in (_DEVICE_TYPE_LIGHT_NON_DIMMABLE, _DEVICE_TYPE_SWITCH):
            transition_time = 0

        # Queue command for processing (state will be updated after confirmation)
        async def enqueue_command():
            if light_on:
                if brightness == 255 and transition_time == 0:
                    # lighting on
                    mqtt_state = {
                        'state': 'ON',
                        'brightness': 255,
                        'transition': 0,
                        'device_type': device_type
                    }
                    await userdata.queue_command(
                        'on', ga, device_type, {}, mqtt_state)
                else:
                    # ramp
                    mqtt_state = {
                        'state': 'ON' if brightness > 0 else 'OFF',
                        'brightness': brightness,
                        'transition': transition_time,
                        'device_type': device_type
                    }
                    await userdata.queue_command(
                        'ramp', ga, device_type,
                        {'duration': transition_time, 'level': brightness},
                        mqtt_state)
            else:
                # lighting off
                if transition_time > 0:
                    # ramp down to 0 over the transition period
                    mqtt_state = {
                        'state': 'OFF',
                        'brightness': 0,
                        'transition': transition_time,
                        'device_type': device_type
                    }
                    await userdata.queue_command(
                        'ramp', ga, device_type,
                        {'duration': transition_time, 'level': 0},
                        mqtt_state)
                else:
                    # immediate off
                    mqtt_state = {
                        'state': 'OFF',
                        'brightness': 0,
                        'transition': 0,
                        'device_type': device_type
                    }
                    await userdata.queue_command(
                        'off', ga, device_type, {}, mqtt_state)
        
        userdata._start_task(enqueue_command(), 'cbus-enqueue-command')

    def publish(self, topic: Text, payload: Dict[Text, Any]):
        """Publishes a payload as JSON."""
        payload = json.dumps(payload)
        return self._publish_raw(topic, payload)

    def _publish_raw(self, topic, payload, qos=1, retain=True):
        if retain and topic.endswith(_TOPIC_STATE_SUFFIX):
            self._state_cache[topic] = payload
        if self._asyncio_loop is not None and not self.is_connected():
            info = mqtt.MQTTMessageInfo(0)
            info.rc = mqtt.MQTT_ERR_NO_CONN
        else:
            info = super().publish(topic, payload, qos=qos, retain=retain)
        if isinstance(info.rc, int) and info.rc != mqtt.MQTT_ERR_SUCCESS:
            if not self._publish_failed:
                logger.warning('MQTT publish unavailable (rc=%s); latest states cached, live events cannot be replayed from state', info.rc)
                self._publish_failed = True
        return info

    def publish_lighting_event(self, event: str, source_addr: int,
                               group_addr: int, duration: Optional[int] = None,
                               level: Optional[int] = None):
        """Publish an observed Application 56 command, without retaining it."""
        payload = {
            'event': event,
            'source_unit': source_addr,
            'group': group_addr,
            'application': int(Application.LIGHTING),
        }
        if event == 'ramp':
            payload.update(level=level, duration=duration)
        # Do not use self.publish(): discovery and state intentionally retain.
        if self._asyncio_loop is not None and not self.is_connected():
            self._offline_events += 1
            if self._offline_events == 1:
                logger.warning('MQTT offline; live lighting event unavailable')
            info = mqtt.MQTTMessageInfo(0)
            info.rc = mqtt.MQTT_ERR_NO_CONN
            return info
        return self._publish_raw(
            _LIGHTING_EVENT_TOPIC_PREFIX + str(group_addr),
            json.dumps(payload), qos=1, retain=False)

    def publish_all_lights(self, labels: Dict[int, Text], 
                          device_types: Dict[int, str]):
        """Publishes configuration for all devices based on their types."""
        # Meta-device which holds all the C-Bus group addresses
        self.publish(_META_TOPIC + _TOPIC_CONF_SUFFIX, {
            '~': _META_TOPIC,
            'name': 'cbus2ha',
            'unique_id': 'cbus2ha',
            'stat_t': '~' + _TOPIC_STATE_SUFFIX,  # unused
            'device': {
                'identifiers': ['cbus2ha'],
                'sw_version': 'cbus2ha https://github.com/wazza-aus/cbus2ha',
                'name': 'cbus2ha',
                'manufacturer': 'Clipsal CBus Home Automation',
                'model': 'cbus2ha',
            },
        })

        for ga in ga_range():
            device_type = get_device_type(ga, device_types)
            
            # Skip ignored devices completely
            if device_type == _DEVICE_TYPE_IGNORE:
                continue
            
            name = labels.get(ga, f'C-Bus {device_type.replace("_", " ").title()} {ga:03d}')
            
            if device_type == _DEVICE_TYPE_LIGHT:
                self._publish_light_config(ga, name, dimmable=True)
            elif device_type == _DEVICE_TYPE_LIGHT_NON_DIMMABLE:
                self._publish_light_config(ga, name, dimmable=False)
            elif device_type == _DEVICE_TYPE_SWITCH:
                self._publish_switch_config(ga, name)
            elif device_type == _DEVICE_TYPE_BINARY_SENSOR:
                self._publish_binary_sensor_config(ga, name)

    def _publish_light_config(self, ga: int, name: str, dimmable: bool = True):
        """Publish light entity configuration."""
        config = {
            'name': name,
            'unique_id': f'cbus_light_{ga}',
            'cmd_t': set_topic(ga),
            'stat_t': state_topic(ga),
            'schema': 'json',
            'brightness': dimmable,  # Key difference!
            'device': {
                'identifiers': [f'cbus_light_{ga}'],
                'connections': [['cbus_group_address', str(ga)]],
                'sw_version': 'cbus2ha https://github.com/wazza-aus/cbus2ha',
                'name': f'C-Bus Light {ga:03d}',
                'manufacturer': 'Clipsal CBus Home Automation',
                'model': 'cbus2ha',
                'via_device': 'cbus2ha',
            },
        }
        # Add supported_color_modes based on dimmable capability
        if dimmable:
            config['supported_color_modes'] = ['brightness']
        else:
            config['supported_color_modes'] = ['onoff']
        self.publish(conf_topic(ga), config)

    def _publish_switch_config(self, ga: int, name: str):
        """Publish switch entity configuration."""
        topic = _SWITCH_TOPIC_PREFIX + str(ga) + _TOPIC_CONF_SUFFIX
        self.publish(topic, {
            'name': name,
            'unique_id': f'cbus_switch_{ga}',
            'cmd_t': _SWITCH_TOPIC_PREFIX + str(ga) + _TOPIC_SET_SUFFIX,
            'stat_t': _SWITCH_TOPIC_PREFIX + str(ga) + _TOPIC_STATE_SUFFIX,
            'schema': 'json',
            'device': {
                'identifiers': [f'cbus_switch_{ga}'],
                'connections': [['cbus_group_address', str(ga)]],
                'sw_version': 'cbus2ha https://github.com/wazza-aus/cbus2ha',
                'name': f'C-Bus Switch {ga:03d}',
                'manufacturer': 'Clipsal CBus Home Automation',
                'model': 'cbus2ha',
                'via_device': 'cbus2ha',
            },
        })

    def _publish_binary_sensor_state_tracker(self, ga: int, name: str):
        """Publish binary sensor for state tracking (existing behavior)."""
        self.publish(bin_sensor_conf_topic(ga), {
            'name': f'{name} (as binary sensor)',
            'unique_id': f'cbus_bin_sensor_{ga}',
            'stat_t': bin_sensor_state_topic(ga),
            'device': {
                'identifiers': [f'cbus_bin_sensor_{ga}'],
                'connections': [['cbus_group_address', str(ga)]],
                'sw_version': 'cbus2ha https://github.com/wazza-aus/cbus2ha',
                'name': f'C-Bus Light {ga:03d}',
                'manufacturer': 'Clipsal CBus Home Automation',
                'model': 'cbus2ha',
                'via_device': 'cbus2ha',
            },
        })

    def _publish_binary_sensor_config(self, ga: int, name: str):
        """Publish binary sensor entity configuration (read-only)."""
        self.publish(bin_sensor_conf_topic(ga), {
            'name': name,
            'unique_id': f'cbus_binary_sensor_{ga}',
            'stat_t': bin_sensor_state_topic(ga),
            'device': {
                'identifiers': [f'cbus_binary_sensor_{ga}'],
                'connections': [['cbus_group_address', str(ga)]],
                'sw_version': 'cbus2ha https://github.com/wazza-aus/cbus2ha',
                'name': f'C-Bus Binary Sensor {ga:03d}',
                'manufacturer': 'Clipsal CBus Home Automation',
                'model': 'cbus2ha',
                'via_device': 'cbus2ha',
            },
        })

    def publish_binary_sensor(self, group_addr: int, state: bool):
        payload = 'ON' if state else 'OFF'
        return self._publish_raw(bin_sensor_state_topic(group_addr), payload)

    def lighting_group_on(self, source_addr: Optional[int], group_addr: int,
                          device_type: Optional[str] = None):
        """Relays a lighting-on event from CBus to MQTT."""
        if device_type is None:
            device_type = _DEVICE_TYPE_LIGHT  # Default for backward compatibility
        
        # Binary sensors only publish to binary sensor topic
        if device_type == _DEVICE_TYPE_BINARY_SENSOR:
            self.publish_binary_sensor(group_addr, True)
            return
        
        state_topic_str = state_topic_for_device(group_addr, device_type)
        
        # Switches need plain string state updates, lights need JSON
        if device_type == _DEVICE_TYPE_SWITCH:
            # Publish plain string for switches
            self._publish_raw(state_topic_str, 'ON')
        else:
            # Publish JSON for lights
            payload = {
                'state': 'ON',
                'brightness': 255,
                'transition': 0,
                'cbus_source_addr': source_addr,
            }
            # Set color_mode based on whether light is dimmable
            if device_type == _DEVICE_TYPE_LIGHT_NON_DIMMABLE:
                payload['color_mode'] = 'onoff'
            else:
                payload['color_mode'] = 'brightness'
            self.publish(state_topic_str, payload)

    def lighting_group_off(self, source_addr: Optional[int], group_addr: int,
                           device_type: Optional[str] = None):
        """Relays a lighting-off event from CBus to MQTT."""
        if device_type is None:
            device_type = _DEVICE_TYPE_LIGHT
        
        # Binary sensors only publish to binary sensor topic
        if device_type == _DEVICE_TYPE_BINARY_SENSOR:
            self.publish_binary_sensor(group_addr, False)
            return
        
        state_topic_str = state_topic_for_device(group_addr, device_type)
        
        # Switches need plain string state updates, lights need JSON
        if device_type == _DEVICE_TYPE_SWITCH:
            # Publish plain string for switches
            self._publish_raw(state_topic_str, 'OFF')
        else:
            # Publish JSON for lights
            payload = {
                'state': 'OFF',
                'brightness': 0,
                'transition': 0,
                'cbus_source_addr': source_addr,
            }
            # Set color_mode based on whether light is dimmable
            if device_type == _DEVICE_TYPE_LIGHT_NON_DIMMABLE:
                payload['color_mode'] = 'onoff'
            else:
                payload['color_mode'] = 'brightness'
            self.publish(state_topic_str, payload)

    def lighting_group_ramp(self, source_addr: Optional[int], group_addr: int,
                           duration: int, level: int, device_type: Optional[str] = None):
        """Relays a lighting-ramp event from CBus to MQTT."""
        if device_type is None:
            device_type = _DEVICE_TYPE_LIGHT
        
        # Binary sensors only publish to binary sensor topic
        if device_type == _DEVICE_TYPE_BINARY_SENSOR:
            self.publish_binary_sensor(group_addr, level > 0)
            return
        
        state_topic_str = state_topic_for_device(group_addr, device_type)
        state = 'OFF' if level == 0 else 'ON'
        
        # Switches need plain string state updates, lights need JSON
        if device_type == _DEVICE_TYPE_SWITCH:
            # Publish plain string for switches
            self._publish_raw(state_topic_str, state)
        else:
            # Publish JSON for lights
            payload = {
                'state': state,
                'brightness': level,
                'transition': duration,
                'cbus_source_addr': source_addr,
            }
            # Set color_mode based on whether light is dimmable
            if device_type == _DEVICE_TYPE_LIGHT_NON_DIMMABLE:
                payload['color_mode'] = 'onoff'
            else:
                payload['color_mode'] = 'brightness'
            self.publish(state_topic_str, payload)


def read_auth(client: mqtt.Client, auth_file: TextIO):
    """Reads authentication from a file."""
    username = auth_file.readline().strip()
    password = auth_file.readline().strip()
    client.username_pw_set(username, password)


def read_cbz_labels(cbz_file: BinaryIO) -> Dict[int, Text]:
    """Reads group address names from a given Toolkit CBZ file."""
    labels = {}  # type: Dict[int, Text]
    cbz = CBZ(cbz_file)

    # TODO: support multiple networks/applications
    # Look for 1 direct network
    networks = [n for n in cbz.installation.project.network
                if n.interface.interface_type != 'bridge']
    if len(networks) != 1:
        logger.warning('Expected exactly 1 non-bridge network in project file, '
                       'got %d instead! Labels will be unavailable.',
                       len(networks))
        return labels

    # Look for
    applications = [a for a in networks[0].applications
                    if a.address == Application.LIGHTING]
    if len(applications) != 1:
        logger.warning('Could not find lighting application %x in project '
                       'file. Labels will be unavailable.',
                       Application.LIGHTING)
        return labels

    for group in applications[0].groups:
        name = group.tag_name.strip()

        # Ignore default names
        if not name or name in ('<Unused>', f'Group {group.address}'):
            continue

        labels[group.address] = name

    return labels


async def _main():
    parser = ArgumentParser()

    group = parser.add_argument_group('Logging options')
    group.add_argument(
        '-l', '--log-file',
        dest='log', default=None,
        help='Destination to write logs [default: stdout]')

    group.add_argument(
        '-v', '--verbosity',
        dest='verbosity', default='INFO', choices=(
            'CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'),
        help='Verbosity of logging to emit [default: %(default)s]')

    group = parser.add_argument_group('MQTT options')
    group.add_argument(
        '--lighting-events',
        action='store_true', default=False,
        help='Publish received Application 56 commands to cbus/events/lighting/'
             '<group> (QoS 1, not retained). Disabled by default.')

    group.add_argument(
        '-b', '--broker-address',
        required=True,
        help='Address of the MQTT broker')

    group.add_argument(
        '-p', '--broker-port',
        type=int, default=0,
        help='Port to use to connect to the MQTT broker. [default: 8883 if '
             'using TLS (default), otherwise 1883]')

    group.add_argument(
        '--broker-keepalive',
        type=int, default=60, metavar='SECONDS',
        help='Send a MQTT keep-alive message every n seconds. Most people '
             'should not need to change this. [default: %(default)s seconds]')

    group.add_argument(
        '--broker-disable-tls',
        action='store_true',
        help='Disables TLS [default: TLS is enabled]. Setting this option is '
             'insecure.')

    group.add_argument(
        '-A', '--broker-auth',
        type=FileType('rt'),
        help='File containing the username and password to authenticate to the '
             'MQTT broker with. The first line in the file is the username, '
             'and the second line is the password. The file must be UTF-8 '
             'encoded. If not specified, authentication will be disabled '
             '(insecure!)')

    group.add_argument(
        '-c', '--broker-ca',
        help='Path to directory containing CA certificates to trust. If not '
             'specified, the default (Python) CA store is used instead.')

    group.add_argument(
        '-k', '--broker-client-cert',
        help='Path to PEM-encoded client certificate (public part). If not '
             'specified, client authentication will not be used. Must also '
             'supply the private key (-K).')

    group.add_argument(
        '-K', '--broker-client-key',
        help='Path to PEM-encoded client key (private part). If not '
             'specified, client authentication will not be used. Must also '
             'supply the public key (-k). If this file is encrypted, Python '
             'will prompt for the password at the command-line.')

    group = parser.add_argument_group(
        'C-Bus PCI options', 'You must specify exactly one of these options:')
    group = group.add_mutually_exclusive_group(required=True)

    group.add_argument(
        '-s', '--serial',
        dest='serial', default=None, metavar='DEVICE',
        help='Device node that the PCI is connected to. USB PCIs act as a '
             'cp210x USB-serial adapter. (example: -s /dev/ttyUSB0)')

    group.add_argument(
        '-t', '--tcp',
        dest='tcp', default=None, metavar='ADDR:PORT',
        help='IP address and TCP port where the C-Bus CNI or PCI is located '
             '(eg: -t 192.0.2.1:10001)')

    group = parser.add_argument_group('Connection health')
    group.add_argument(
        '--link-check-interval', type=float, default=60, metavar='SECONDS',
        help='Interval between acknowledged read-only PCI probes (0 disables). '
             '[default: %(default)s seconds]')

    group = parser.add_argument_group('Time settings')
    group.add_argument(
        '-T', '--timesync', metavar='SECONDS',
        dest='timesync', type=int, default=300,
        help='Send time synchronisation packets every n seconds '
             '(or 0 to disable). [default: %(default)s seconds]')

    group.add_argument(
        '-C', '--no-clock',
        dest='no_clock', action='store_true',
        default=False,
        help='Do not respond to Clock Request SAL messages with the system '
             'time (ie: do not provide the CBus network the time when '
             'requested). Enable if your machine does not have a reliable '
             'time source, or you have another device on the CBus network '
             'providing time services. [default: %(default)s]')

    group = parser.add_argument_group('Label options')

    group.add_argument(
        '-P', '--project-file',
        type=FileType('rb'),
        help='Path to a C-Bus Toolkit project backup file (CBZ or XML) '
             'containing labels for group addresses to use. If not supplied, '
             'generated names like "C-Bus Light 001" will be used instead.'
    )

    group = parser.add_argument_group('Device type configuration')
    group.add_argument(
        '--non-dimmable-lights',
        dest='non_dimmable_lights', default='',
        help='Comma-separated list of group addresses for non-dimmable lights '
             '(e.g., "26,65,81")')
    group.add_argument(
        '--switches',
        dest='switches', default='',
        help='Comma-separated list of group addresses for switches '
             '(e.g., "15,90")')
    group.add_argument(
        '--binary-sensors',
        dest='binary_sensors', default='',
        help='Comma-separated list of group addresses for binary sensors '
             '(read-only state tracking, e.g., "10,20,30")')
    group.add_argument(
        '--ignore',
        dest='ignore', default='',
        help='Comma-separated list of group addresses to ignore '
             '(no MQTT discovery or subscriptions, e.g., "5,15,25")')

    option = parser.parse_args()
    if option.broker_keepalive <= 0:
        parser.error('--broker-keepalive must be positive for failure detection')
    if option.link_check_interval < 0:
        parser.error('--link-check-interval must be non-negative')

    if bool(option.broker_client_cert) != bool(option.broker_client_key):
        return parser.error(
            'To use client certificates, both -k and -K must be specified.')

    global_logger = logging.getLogger('cbus')
    global_logger.setLevel(option.verbosity)
    logging.basicConfig(level=option.verbosity, filename=option.log,
                        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
                        force=True)
    
    # Also cover execution as __main__, outside the cbus logger hierarchy.
    logger.setLevel(option.verbosity)

    loop = get_event_loop()
    labels = (read_cbz_labels(option.project_file)
              if option.project_file else None)

    # Parse device type configurations
    device_types = {}  # group_addr -> device_type_string

    def parse_device_list(ga_list_str: str, device_type: str):
        """Parse comma-separated group addresses and assign device type."""
        if not ga_list_str:
            return
        for ga_str in ga_list_str.split(','):
            try:
                ga = int(ga_str.strip())
                check_ga(ga)
                device_types[ga] = device_type
            except (ValueError, TypeError):
                logger.warning(f'Invalid group address in {device_type}: {ga_str}')

    parse_device_list(option.non_dimmable_lights, _DEVICE_TYPE_LIGHT_NON_DIMMABLE)
    parse_device_list(option.switches, _DEVICE_TYPE_SWITCH)
    parse_device_list(option.binary_sensors, _DEVICE_TYPE_BINARY_SENSOR)
    parse_device_list(option.ignore, _DEVICE_TYPE_IGNORE)

    def factory():
        return CBusHandler(
            timesync_frequency=option.timesync,
            handle_clock_requests=not option.no_clock,
            connection_lost_future=loop.create_future(),
            labels=labels,
            device_types=device_types,
            lighting_events=option.lighting_events,
            link_check_interval=option.link_check_interval,
        )

    if option.serial:
        async def connect(protocol_factory):
            return await create_serial_connection(
                loop, protocol_factory, option.serial, baudrate=9600)
    else:
        addr, tcp_port = option.tcp.rsplit(':', 1)
        async def connect(protocol_factory):
            return await loop.create_connection(protocol_factory, addr, int(tcp_port))

    # Metadata is available for discovery even if the first C-Bus connection
    # fails. Only a successfully connected protocol accepts MQTT commands.
    mqtt_client = MqttClient(userdata=factory())
    mqtt_client.attach_handler(None)
    if option.broker_auth:
        read_auth(mqtt_client, option.broker_auth)
    if option.broker_disable_tls:
        logging.warning('Transport security disabled!')
        port = option.broker_port or 1883
    else:
        tls_args = {}
        if option.broker_ca:
            tls_args['ca_certs'] = option.broker_ca
        if option.broker_client_cert:
            tls_args['certfile'] = option.broker_client_cert
            tls_args['keyfile'] = option.broker_client_key
        mqtt_client.tls_set(**tls_args)
        port = option.broker_port or 8883

    stopping = asyncio.Event()

    def request_shutdown():
        logger.info('Shutdown requested')
        stopping.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, request_shutdown)
    # Stack traces contain no payloads or credential values. Signal the Python
    # process itself (not the container's init process) when diagnosing a stall.
    faulthandler.register(signal.SIGUSR1, all_threads=True)
    tasks = []
    try:
        mqtt_client.start(loop, option.broker_address, port, option.broker_keepalive)
        tasks = [
            create_task(maintain_cbus_connection(factory, connect, mqtt_client)),
            create_task(log_health(mqtt_client)),
            create_task(stopping.wait()),
        ]
        done, _ = await asyncio.wait(
            tasks + [mqtt_client.network_failure], return_when=asyncio.FIRST_COMPLETED)
        if mqtt_client.network_failure in done:
            raise RuntimeError('MQTT network driver stopped unexpectedly')
        for task in done:
            task.result()
        if not stopping.is_set():
            raise RuntimeError('Bridge supervisor stopped unexpectedly')
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await mqtt_client.stop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)
        faulthandler.unregister(signal.SIGUSR1)
        logger.info('Bridge shutdown complete')


def main():
    # work-around asyncio vs. setuptools console_scripts
    run(_main())


if __name__ == '__main__':
    main()
