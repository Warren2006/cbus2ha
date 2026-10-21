"""Failure injection for connection ownership, live events and recovery.

No broker, hardware, sockets or Docker are needed for this module.
"""

import asyncio
import json
import threading
import time
import unittest
from unittest.mock import Mock, patch

from cbus.common import Application, CONFIRMATION_CODES, GroupState
from cbus.daemon import cmqttd, recovery
from cbus.protocol.cal.extended import ExtendedCAL
from cbus.protocol.cal.report import BinaryStatusReport, LevelStatusReport
from cbus.protocol.pciprotocol import PCIProtocol
from cbus.protocol.pp_packet import PointToPointPacket
from tests.test_lighting_events import PublicationMixin, inbound_packet


async def until(predicate, timeout=2):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError('Condition did not become true')
        await asyncio.sleep(0.002)


class FakePCITransport:
    def __init__(self, protocol, acknowledge=True):
        self.protocol = protocol
        self.acknowledge = acknowledge
        self.writes = []
        self.closed = False

    def write(self, data):
        if self.closed:
            raise OSError('closed fake PCI')
        self.writes.append(data)
        if self.acknowledge and data[-2:-1] in [bytes([c]) for c in CONFIRMATION_CODES]:
            asyncio.get_running_loop().call_soon(
                self.protocol.data_received, data[-2:-1] + b'.\r\n')

    def abort(self):
        if not self.closed:
            self.closed = True
            asyncio.get_running_loop().call_soon(self.protocol.connection_lost, None)


class ReliabilityTest(PublicationMixin, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_publish()
        self.handler, self.client = self.make_bridge()
        self.subscribe_patch = patch.object(self.client, 'subscribe', return_value=(0, 1))
        self.subscribe = self.subscribe_patch.start()
        self.addCleanup(self.subscribe_patch.stop)
        self.handlers = [self.handler]

    async def asyncTearDown(self):
        for handler in self.handlers:
            if handler._transport is not None:
                handler._transport.abort()
            await handler.shutdown()
        await asyncio.sleep(0)

    def managed(self, connected=True):
        self.client._asyncio_loop = asyncio.get_running_loop()
        connected_patch = patch.object(self.client, 'is_connected', return_value=connected)
        self.connected = connected_patch.start()
        self.addCleanup(connected_patch.stop)
        self.client.attach_handler(self.handler)

    def connect_handler(self, acknowledge=True, interval=0.01, timeout=0.01):
        self.handler._timesync_frequency = 0
        self.handler.link_check_interval = interval
        self.handler.link_probe_timeout = timeout
        self.handler._connection_lost_future = asyncio.get_running_loop().create_future()
        transport = FakePCITransport(self.handler, acknowledge)
        self.handler.connection_made(transport)
        return transport

    async def test_repeated_mqtt_reconnect_restores_one_subscription_set_each_time(self):
        transport = self.connect_handler(interval=0)
        self.managed()
        workers = (self.handler.queue_processor_task, self.handler.timeout_watchdog_task)
        for _ in range(5):
            self.client.on_disconnect(self.client, self.handler, 7)
            self.client.on_connect(self.client, self.handler, {}, 0)
            await asyncio.sleep(0)
        self.assertEqual(self.subscribe.call_count, 5)
        topics = self.subscribe.call_args.args[0]
        self.assertEqual(len(topics), 512)
        self.assertEqual(len(set(topics)), 512)
        self.assertEqual(workers, (self.handler.queue_processor_task,
                                   self.handler.timeout_watchdog_task))
        self.assertFalse(transport.closed)
        self.assertEqual(self.publications(events=True), [])

    async def test_rejected_connack_does_not_publish_or_subscribe(self):
        self.managed()
        with self.assertLogs(cmqttd.logger, 'WARNING') as logs:
            self.client.on_connect(self.client, self.handler, {}, 5)
            await asyncio.sleep(0)
        self.assertIn('refused', '\n'.join(logs.output))
        self.subscribe.assert_not_called()
        self.publish_mock.assert_not_called()

    async def test_offline_states_coalesce_and_resync_without_button_events(self):
        self.managed(connected=False)
        for i in range(100):
            self.handler.data_received(inbound_packet(b'\x0a\xc8' + bytes([i])))
        self.assertEqual(len(self.client._state_cache), 1)
        self.assertEqual(self.client._offline_events, 100)
        self.publish_mock.assert_not_called()
        self.connected.return_value = True
        self.client.on_connect(self.client, self.handler, {}, 0)
        await asyncio.sleep(0)
        states = [p for p in self.publications(False) if p[0].endswith('/state')]
        self.assertEqual(len(states), 1)
        self.assertEqual(states[0][1]['brightness'], 99)
        self.assertEqual(self.publications(True), [])

    async def test_plain_switch_and_binary_states_are_also_restored(self):
        self.managed(connected=False)
        self.client.lighting_group_on(None, 5, 'switch')
        self.client.lighting_group_ramp(None, 5, 0, 0, 'switch')
        self.client.publish_binary_sensor(6, True)
        self.connected.return_value = True
        self.client.on_connect(self.client, self.handler, {}, 0)
        await asyncio.sleep(0)
        states = [p[:2] for p in self.publications(False) if p[0].endswith('/state')]
        self.assertEqual(states, [('homeassistant/switch/cbus_5/state', 'OFF'),
                                  ('homeassistant/binary_sensor/cbus_6/state', 'ON')])
        self.assertEqual(self.publications(True), [])

    async def test_invalid_payload_is_logged_and_next_command_is_processed(self):
        self.connect_handler(interval=0)
        self.managed()
        msg = cmqttd.mqtt.MQTTMessage(topic=b'homeassistant/light/cbus_200/set')
        with self.assertLogs(cmqttd.logger, 'WARNING'):
            for payload in (b'{}', b'[]', b'null', b'"ON"', b'{"state":null}',
                            b'{"state":"ON","brightness":"bad"}'):
                msg.payload = payload
                self.client.on_message(self.client, None, msg)
                await asyncio.sleep(0)
        self.assertEqual(self.handler.command_queue.qsize(), 0)
        msg.payload = b'{"state":"ON"}'
        self.client.on_message(self.client, None, msg)
        await until(lambda: bool(self.publications(False)))
        self.assertEqual(len(self.publications(False)), 1)
        self.assertEqual(self.publications(True), [])

    async def test_callback_exception_cannot_escape_to_network_thread(self):
        self.managed()
        def broken():
            raise RuntimeError('injected callback error')
        with self.assertLogs(cmqttd.logger, 'ERROR'):
            self.client._dispatch(broken)
            await asyncio.sleep(0)
        self.assertFalse(self.client._stopping)

    async def test_slow_mqtt_connect_runs_off_loop_and_does_not_pause_bus(self):
        entered = threading.Event()
        released = threading.Event()
        threads = []
        def slow_connect(_client):
            threads.append(threading.get_ident())
            entered.set()
            released.wait(2)
            raise OSError('injected unreachable broker')
        with patch.object(cmqttd.mqtt.Client, 'reconnect', slow_connect):
            self.client.start(asyncio.get_running_loop(), 'test.invalid', 1883, 1)
            try:
                await until(entered.is_set)
                for _ in range(10):
                    self.handler.data_received(inbound_packet(b'\x79\xc8'))
                    await asyncio.sleep(0.002)
                self.assertEqual(len(self.client._state_cache), 1)
                self.assertNotEqual(threads[0], threading.get_ident())
                self.assertFalse(released.is_set())
            finally:
                released.set()
                await self.client.stop()
        self.assertIsNone(self.client._thread)

    async def test_network_callbacks_execute_on_asyncio_thread(self):
        self.managed()
        called = []
        with patch.object(self.client, '_handle_connect',
                          side_effect=lambda *_: called.append(threading.get_ident())):
            thread = threading.Thread(target=lambda: self.client.on_connect(
                self.client, self.handler, {}, 0))
            thread.start()
            thread.join()
            await until(lambda: bool(called))
        self.assertEqual(called, [threading.get_ident()])

    async def test_shutdown_wait_is_bounded_when_mqtt_connect_is_stalled(self):
        entered, released = threading.Event(), threading.Event()
        def stalled(_client):
            entered.set()
            released.wait(2)
            raise OSError('injected slow resolver')
        with patch.object(cmqttd.mqtt.Client, 'reconnect', stalled):
            self.client.start(asyncio.get_running_loop(), 'test.invalid', 1883, 1)
            try:
                await until(entered.is_set)
                started = time.monotonic()
                with self.assertLogs(cmqttd.logger, 'WARNING'):
                    await self.client.stop(timeout=0.01)
                self.assertLess(time.monotonic() - started, 0.2)
            finally:
                released.set()
                await until(lambda: self.client._thread is None)

    async def test_initialisation_write_failure_closes_transport(self):
        transport = FakePCITransport(self.handler)
        with patch.object(transport, 'write', side_effect=OSError('injected write failure')):
            with self.assertLogs(cmqttd.logger, 'ERROR'):
                self.handler.connection_made(transport)
                await asyncio.sleep(0)
        self.assertTrue(transport.closed)
        self.assertFalse(self.handler._tasks)

    async def test_rejected_suback_is_visible_and_not_marked_ready(self):
        self.managed()
        with self.assertLogs(cmqttd.logger, 'ERROR'):
            self.client.on_subscribe(self.client, None, 1, [2, 128])
            await asyncio.sleep(0)
        self.assertFalse(self.client.subscriptions_ready)

    async def test_idle_bus_answers_probes_without_recovery_or_events(self):
        transport = self.connect_handler()
        await asyncio.sleep(0.08)
        self.assertFalse(transport.closed)
        self.assertGreater(len(transport.writes), 8)
        self.assertEqual(self.publications(True), [])

    async def test_silent_interface_requires_two_failed_probes_and_logs(self):
        with self.assertLogs(cmqttd.logger, 'WARNING') as logs:
            transport = self.connect_handler(acknowledge=False)
            await until(lambda: transport.closed)
            await asyncio.sleep(0)
        output = '\n'.join(logs.output)
        self.assertIn('(1/2)', output)
        self.assertIn('(2/2)', output)
        self.assertIn('two failed PCI probes', output)
        self.assertEqual(self.publications(True), [])

    async def test_unrelated_physical_traffic_cannot_satisfy_a_probe(self):
        transport = self.connect_handler(acknowledge=False)
        for _ in range(30):
            if transport.closed:
                break
            self.handler.data_received(inbound_packet(b'\x79\xc8'))
            await asyncio.sleep(0.003)
        self.assertTrue(transport.closed)
        self.assertGreater(len(self.publications(True)), 0)

    async def test_probe_code_cannot_be_reused_by_queued_or_system_commands(self):
        self.handler._probe_code = b'g'
        self.handler.pending_confirmations[b'h'] = cmqttd.QueuedCommand('on', 1, 'light')
        codes = [self.handler._get_confirmation_code() for _ in range(100)]
        self.assertNotIn(b'g', codes)
        self.assertNotIn(b'h', codes)

    async def test_reboot_notifications_are_coalesced(self):
        transport = self.connect_handler(interval=0)
        with self.assertLogs(cmqttd.logger, 'WARNING') as logs:
            self.handler.data_received(b'++')
            await asyncio.sleep(0)
        self.assertTrue(transport.closed)
        self.assertEqual(sum('recovery requested' in line for line in logs.output), 1)
        self.assertEqual(self.publications(True), [])

    async def test_status_replies_never_become_physical_events(self):
        for report in (BinaryStatusReport([GroupState.ON, GroupState.OFF]),
                       LevelStatusReport([0, 127, 255])):
            status = PointToPointPacket(unit_address=46, cals=[
                ExtendedCAL(False, Application.LIGHTING, 200, report)])
            self.handler.data_received(status.encode_packet() + b'\r\n')
        self.publish_mock.assert_not_called()
        self.handler.data_received(inbound_packet(b'\x79\xc8\x01\xc8\x0a\xc8\x7f\x09\xc8'))
        self.assertEqual([p[1]['event'] for p in self.publications(True)],
                         ['on', 'off', 'ramp', 'terminate_ramp'])

    async def test_bad_input_is_logged_then_next_physical_frame_works(self):
        with patch.object(self.handler, 'handle_data', side_effect=ValueError('bad frame')):
            with self.assertLogs(cmqttd.logger, 'ERROR'):
                self.handler.data_received(b'broken\r\n')
        self.assertEqual(self.handler._buf, bytearray())
        self.handler.data_received(inbound_packet(b'\x79\xc8'))
        self.assertEqual(len(self.publications(True)), 1)

    async def test_disconnect_discards_commands_and_joins_owned_tasks(self):
        transport = self.connect_handler(interval=0)
        self.handler.start_queue_system()
        workers = set(self.handler._tasks)
        self.assertEqual(len(workers), 2)
        await self.handler.queue_command('on', 1, 'light', {}, {'state': 'ON'})
        self.handler.pending_confirmations[b'x'] = cmqttd.QueuedCommand('off', 2, 'light')
        transport.abort()
        await asyncio.sleep(0)
        await self.handler.shutdown()
        self.assertTrue(all(t.done() for t in workers))
        self.assertEqual(self.handler._tasks, set())
        self.assertEqual(self.handler.pending_confirmations, {})
        self.assertEqual(self.handler.command_queue.qsize(), 0)

    async def test_background_task_failure_is_observed_and_recovers_link(self):
        transport = self.connect_handler(interval=0)
        async def fail():
            raise RuntimeError('injected task failure')
        with self.assertLogs(cmqttd.logger, 'ERROR'):
            self.handler._start_task(fail(), 'injected-task')
            await until(lambda: transport.closed)

    async def test_timesync_task_is_cancelled_on_loss_and_optional_future_is_safe(self):
        protocol = PCIProtocol(timesync_frequency=300)
        transport = FakePCITransport(protocol)
        protocol.connection_made(transport)
        await asyncio.sleep(0)
        task = protocol._timesync_task
        protocol.connection_lost(None)
        protocol.connection_lost(None)
        await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(task.done())

    async def test_publish_queue_failure_has_normal_level_diagnostic(self):
        self.managed()
        info = cmqttd.mqtt.MQTTMessageInfo(0)
        info.rc = cmqttd.mqtt.MQTT_ERR_QUEUE_SIZE
        self.publish_mock.return_value = info
        with self.assertLogs(cmqttd.logger, 'WARNING') as logs:
            for _ in range(10):
                self.client.lighting_group_on(46, 200)
        self.assertEqual(len(logs.output), 1)
        self.assertEqual(self.client._max_queued_messages, 1024)
        self.assertEqual(len(self.client._state_cache), 1)

    async def test_tcp_disconnect_and_repeated_reconnect_use_fresh_protocols(self):
        self.managed()
        def factory():
            handler = cmqttd.CBusHandler(labels={}, lighting_events=True,
                timesync_frequency=0, link_check_interval=0,
                connection_lost_future=asyncio.get_running_loop().create_future())
            self.handlers.append(handler)
            return handler
        async def connect(make_protocol):
            handler = make_protocol()
            transport = FakePCITransport(handler)
            handler.connection_made(transport)
            return transport, handler
        supervisor = asyncio.create_task(recovery.maintain_cbus_connection(
            factory, connect, self.client, retry_min=0.002, retry_max=0.01))
        old = None
        try:
            for _ in range(5):
                await until(lambda: self.client.bridge_handler not in (None, old, self.handler))
                current = self.client.bridge_handler
                current.data_received(inbound_packet(b'\x79\xc8'))
                current._transport.abort()
                # Overlapping broker recovery must not restart old C-Bus tasks.
                self.client.on_disconnect(self.client, old, 7)
                self.client.on_connect(self.client, old, {}, 0)
                old = current
            await until(lambda: not old._tasks)
        finally:
            supervisor.cancel()
            await asyncio.gather(supervisor, return_exceptions=True)
        self.assertIsNone(self.client.bridge_handler)
        self.assertEqual(len(self.publications(True)), 5)
        for handler in self.handlers[1:]:
            self.assertFalse(handler._tasks)
            self.assertIsNone(handler._transport)

    async def test_initial_connect_timeout_cleans_partial_transport_and_retries(self):
        self.managed()
        transports = []
        def factory():
            handler = cmqttd.CBusHandler(labels={}, timesync_frequency=0,
                link_check_interval=0,
                connection_lost_future=asyncio.get_running_loop().create_future())
            self.handlers.append(handler)
            return handler
        async def connect(make_protocol):
            handler = make_protocol()
            transport = FakePCITransport(handler)
            handler.connection_made(transport)
            transports.append(transport)
            await asyncio.Event().wait()
        task = asyncio.create_task(recovery.maintain_cbus_connection(
            factory, connect, self.client, retry_min=0.002,
            retry_max=0.004, connect_timeout=0.01))
        try:
            with self.assertLogs(recovery.logger, 'WARNING'):
                await until(lambda: len(transports) >= 3)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(all(t.closed for t in transports))
        self.assertTrue(all(not h._tasks for h in self.handlers))

    async def test_health_summary_records_both_links(self):
        self.managed(connected=False)
        self.client.attach_handler(None)
        task = asyncio.create_task(recovery.log_health(self.client, interval=0.005))
        with self.assertLogs(recovery.logger, 'INFO') as logs:
            await asyncio.sleep(0.02)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertIn('MQTT=disconnected C-Bus=disconnected', '\n'.join(logs.output))

    async def test_failed_connect_attempts_have_capped_backoff_and_cleanup(self):
        delays = []
        async def fail(_factory):
            raise ConnectionRefusedError('injected unavailable interface')
        async def backoff(delay):
            delays.append(delay)
            if len(delays) == 4:
                raise asyncio.CancelledError()
        def factory():
            handler = cmqttd.CBusHandler(labels=None, timesync_frequency=0)
            self.handlers.append(handler)
            return handler
        with patch.object(recovery.asyncio, 'sleep', side_effect=backoff):
            with self.assertRaises(asyncio.CancelledError):
                await recovery.maintain_cbus_connection(
                    factory, fail, self.client, retry_min=1, retry_max=4)
        self.assertEqual(delays, [1, 2, 4, 4])
        self.assertTrue(all(not h._tasks for h in self.handlers))
