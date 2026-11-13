"""Opt-in real MQTT/TCP tests, using only loopback and simulated C-Bus.

Run: CBUS_INTEGRATION=1 python -m unittest -v tests.test_reliability_integration
Requires Docker, its cached/pullable Mosquitto image, and local socket access.
"""

import asyncio
import os
import socket
import signal
import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import paho.mqtt.client as mqtt

from cbus.common import CONFIRMATION_CODES
from cbus.daemon.cmqttd import CBusHandler, MqttClient
from cbus.daemon.recovery import maintain_cbus_connection
from tests.test_lighting_events import inbound_packet
from tests.test_reliability import until


IMAGE = 'eclipse-mosquitto@sha256:199ea8ef2e35ec2b1b37e59cfd1dbae538ed4dfa4a2251a121a52215a6248a21'


@unittest.skipUnless(os.environ.get('CBUS_INTEGRATION') == '1', 'opt-in Docker/loopback test')
class ConnectionIntegrationTest(unittest.IsolatedAsyncioTestCase):
    async def command(self, *args):
        process = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(process.communicate(), 30)
        if process.returncode:
            raise RuntimeError(f'{args[0]} failed: {stderr.decode()}')
        return stdout.decode().strip()

    async def asyncSetUp(self):
        self.loop = asyncio.get_running_loop()
        self.container = None
        self.server = None
        self.observer = None
        self.client = None
        self.supervisor = None
        self.connections = []
        self.handlers = []
        self.messages = []
        self.observer_sessions = 0
        self.observer_subscribed = False
        self.temp = tempfile.TemporaryDirectory(
            prefix='cbus-reliability-', dir=os.environ.get('CBUS_TEST_WORK_DIR'))
        self.addAsyncCleanup(self.cleanup_fixture)
        config = Path(self.temp.name) / 'mosquitto.conf'
        config.write_text('listener 1883\nallow_anonymous true\npersistence false\n')
        # Explicitly pin the chosen free port: Docker's ephemeral mapping can
        # change on restart, which would test endpoint reconfiguration instead.
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            self.broker_port = reservation.getsockname()[1]
        self.container = await self.command(
            'docker', 'run', '--rm', '-d', '-p', f'127.0.0.1:{self.broker_port}:1883',
            '--mount', f'type=bind,source={config},target=/mosquitto/config/mosquitto.conf,readonly', IMAGE)
        self.server = await asyncio.start_server(self.cni, '127.0.0.1', 0)
        self.cni_port = self.server.sockets[0].getsockname()[1]
        self.start_observer()
        await self.start_bridge()
        await until(lambda: self.ready(), timeout=15)
        await until(lambda: len({topic for topic, _ in self.messages
                                if topic.endswith('/config')}) == 257, timeout=15)

    def start_observer(self):
        self.observer = mqtt.Client()
        def connected(client, _userdata, _flags, rc):
            if rc == 0:
                client.subscribe([('homeassistant/#', 1), ('cbus/events/#', 1)])
                self.loop.call_soon_threadsafe(self.observer_connected)
        def received(_client, _userdata, message):
            self.loop.call_soon_threadsafe(self.messages.append, (message.topic, message.payload))
        self.observer.on_connect = connected
        self.observer.on_message = received
        self.observer.on_subscribe = lambda *_: self.loop.call_soon_threadsafe(
            setattr, self, 'observer_subscribed', True)
        self.observer.reconnect_delay_set(1, 2)
        self.observer.connect_async('127.0.0.1', self.broker_port, 2)
        self.observer.loop_start()

    def observer_connected(self):
        self.observer_sessions += 1
        self.observer_subscribed = False

    async def cni(self, reader, writer):
        connection = {'writer': writer, 'silent': False, 'commands': [],
                      'task': asyncio.current_task()}
        self.connections.append(connection)
        try:
            while True:
                data = await reader.readuntil(b'\r')
                connection['commands'].append(data)
                if not connection['silent'] and data[-2:-1] in [bytes([c]) for c in CONFIRMATION_CODES]:
                    writer.write(data[-2:-1] + b'.\r\n')
                    await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def start_bridge(self):
        def factory():
            handler = CBusHandler(labels={}, lighting_events=True, timesync_frequency=0,
                link_check_interval=0.05, link_probe_timeout=0.05,
                connection_lost_future=self.loop.create_future())
            self.handlers.append(handler)
            return handler
        async def connect(make_protocol):
            return await self.loop.create_connection(make_protocol, '127.0.0.1', self.cni_port)
        self.client = MqttClient(userdata=factory())
        self.client.attach_handler(None)
        self.client.start(self.loop, '127.0.0.1', self.broker_port, 2)
        self.supervisor = asyncio.create_task(maintain_cbus_connection(
            factory, connect, self.client, retry_min=0.02, retry_max=0.1))

    async def stop_bridge(self):
        if self.supervisor is not None:
            self.supervisor.cancel()
            await asyncio.gather(self.supervisor, return_exceptions=True)
            self.supervisor = None
        if self.client is not None:
            await self.client.stop()
            self.client = None

    def ready(self):
        return (self.observer.is_connected() and self.client.is_connected()
                and self.observer_subscribed and self.client.subscriptions_ready
                and self.client.bridge_handler is not None
                and self.client.bridge_handler._transport is not None)

    def events(self):
        return [(topic, payload) for topic, payload in self.messages
                if topic.startswith('cbus/events/')]

    async def physical(self, sals=b'\x79\xc8'):
        writer = self.connections[-1]['writer']
        writer.write(inbound_packet(sals))
        await writer.drain()

    async def check_command_and_event(self):
        start = len(self.messages)
        self.observer.publish('homeassistant/light/cbus_201/set', '{"state":"ON"}', qos=1)
        await until(lambda: any(topic == 'homeassistant/light/cbus_201/state'
                               for topic, _ in self.messages[start:]), timeout=5)
        count = len(self.events())
        await self.physical()
        await until(lambda: len(self.events()) > count, timeout=5)
        await asyncio.sleep(0.05)
        self.assertEqual(len(self.events()), count + 1)

    async def cleanup_fixture(self):
        if self.container is not None:
            # Unpause before stopping MQTT clients so shutdown can flush.
            if getattr(self, 'paused', False):
                await self.command('docker', 'unpause', self.container)
        await self.stop_bridge()
        if self.observer is not None:
            self.observer.disconnect()
            await asyncio.to_thread(self.observer.loop_stop)
        for connection in self.connections:
            connection['writer'].close()
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        if self.connections:
            await asyncio.gather(*(c['task'] for c in self.connections), return_exceptions=True)
        if self.container is not None:
            await self.command('docker', 'stop', '-t', '1', self.container)
        self.temp.cleanup()

    async def test_broker_restart_restores_discovery_commands_and_events(self):
        old_session = self.observer_sessions
        self.messages.clear()
        await self.command('docker', 'restart', '-t', '0', self.container)
        await until(lambda: self.observer_sessions > old_session and self.ready(), timeout=15)
        await until(lambda: len({t for t, _ in self.messages if t.endswith('/config')}) == 257, timeout=10)
        self.assertEqual(self.events(), [])
        await self.check_command_and_event()

    async def test_repeated_broker_reconnects_do_not_duplicate_live_events(self):
        for _ in range(3):
            old_session = self.observer_sessions
            await self.command('docker', 'restart', '-t', '0', self.container)
            await until(lambda: self.observer_sessions > old_session and self.ready(), timeout=15)
            await self.check_command_and_event()
        self.assertEqual(len(self.events()), 3)

    async def test_missing_initial_connack_retries_without_blocking_bus(self):
        await self.stop_bridge()
        await self.command('docker', 'pause', self.container)
        self.paused = True
        await self.start_bridge()
        with patch.object(self.client, '_handle_disconnect',
                          wraps=self.client._handle_disconnect) as disconnected:
            await until(lambda: disconnected.called, timeout=10)
        self.assertIsNotNone(self.client.bridge_handler)
        self.assertFalse(self.client.is_connected())
        await self.command('docker', 'unpause', self.container)
        self.paused = False
        await until(lambda: self.ready(), timeout=15)
        await self.check_command_and_event()

    async def test_half_open_broker_detected_by_keepalive_without_socket_eof(self):
        await self.command('docker', 'pause', self.container)
        self.paused = True
        await until(lambda: not self.client.is_connected(), timeout=10)
        # The live C-Bus link and event loop stay operational during broker loss.
        handler = self.client.bridge_handler
        self.assertIsNotNone(handler)
        await self.physical()
        await until(lambda: self.client._offline_events == 1)
        await self.command('docker', 'unpause', self.container)
        self.paused = False
        await until(lambda: self.ready(), timeout=15)
        await asyncio.sleep(0.1)
        self.assertEqual(self.events(), [])
        self.assertIs(self.client.bridge_handler, handler)
        await self.check_command_and_event()

    async def test_tcp_eof_reconnect_and_reinitialisation(self):
        initial = len(self.connections)
        self.connections[-1]['writer'].close()
        await until(lambda: len(self.connections) > initial and self.ready())
        self.assertEqual(self.connections[-1]['commands'][:3], [b'~\r'] * 3)
        self.assertEqual(self.events(), [])
        await self.check_command_and_event()

    async def test_interface_silence_reopens_socket_and_reinitialises(self):
        old = self.client.bridge_handler
        self.connections[-1]['silent'] = True
        await until(lambda: self.client.bridge_handler not in (None, old) and self.ready())
        self.assertEqual(self.events(), [])
        await self.check_command_and_event()

    async def test_interface_power_up_reconnects_once(self):
        initial = len(self.connections)
        writer = self.connections[-1]['writer']
        writer.write(b'++')
        await writer.drain()
        await until(lambda: len(self.connections) > initial and self.ready())
        await asyncio.sleep(0.1)
        self.assertEqual(len(self.connections), initial + 1)
        self.assertEqual(self.events(), [])
        await self.check_command_and_event()

    async def test_overlapping_broker_and_interface_recovery(self):
        old = self.client.bridge_handler
        old_session = self.observer_sessions
        self.connections[-1]['silent'] = True
        await self.command('docker', 'restart', '-t', '0', self.container)
        await until(lambda: self.observer_sessions > old_session
                    and self.client.bridge_handler not in (None, old) and self.ready(), timeout=15)
        self.assertEqual(self.events(), [])
        await self.check_command_and_event()

    async def test_repeated_tcp_recovery_has_no_old_tasks_or_duplicate_events(self):
        for _ in range(5):
            old = self.client.bridge_handler
            self.connections[-1]['writer'].close()
            await until(lambda: self.client.bridge_handler not in (None, old) and self.ready())
            await self.check_command_and_event()
            self.assertEqual(old._tasks, set())
            self.assertIsNone(old._transport)
        self.assertEqual(len(self.events()), 5)

    async def test_ha_mqtt_observer_and_bridge_restart_do_not_replay_events(self):
        await self.check_command_and_event()
        self.observer.disconnect()
        await asyncio.to_thread(self.observer.loop_stop)
        self.messages.clear()
        self.start_observer()
        await until(lambda: self.ready(), timeout=10)
        await asyncio.sleep(0.1)
        self.assertEqual(self.events(), [])  # Button events are not retained.
        await self.stop_bridge()
        await self.start_bridge()
        await until(lambda: self.ready(), timeout=10)
        self.assertEqual(self.events(), [])
        await self.check_command_and_event()

    async def test_real_daemon_process_restart_signals_and_diagnostic_stack_dump(self):
        await self.stop_bridge()
        for iteration in range(2):
            logs = []
            previous_connections = len(self.connections)
            process = await asyncio.create_subprocess_exec(
                sys.executable, '-u', '-m', 'cbus.daemon.cmqttd',
                '--broker-address', '127.0.0.1', '--broker-port', str(self.broker_port),
                '--broker-keepalive', '2', '--broker-disable-tls',
                '--tcp', f'127.0.0.1:{self.cni_port}', '--timesync', '0',
                '--lighting-events', '--link-check-interval', '0.1',
                cwd=str(Path(__file__).resolve().parents[1]),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            async def read_logs():
                while line := await process.stdout.readline():
                    logs.append(line.decode())
            reader = asyncio.create_task(read_logs())
            try:
                await until(lambda: len(self.connections) > previous_connections
                            and any('MQTT command subscriptions restored' in l for l in logs), timeout=10)
                self.assertEqual(len(self.events()), iteration)
                process.send_signal(signal.SIGUSR1)
                await until(lambda: any('cmqttd.py' in l for l in logs), timeout=5)
                await self.check_command_and_event()
                process.send_signal(signal.SIGTERM)
                await asyncio.wait_for(process.wait(), 5)
                await reader
                self.assertEqual(process.returncode, 0)
                self.assertTrue(any('INFO' in l and 'Bridge shutdown complete' in l for l in logs))
            finally:
                if process.returncode is None:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), 5)
                await asyncio.gather(reader, return_exceptions=True)
