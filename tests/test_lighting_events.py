"""Inbound lighting command events and bridge compatibility, without services."""

import asyncio
import json
import unittest
from unittest.mock import Mock, patch

from parameterized import parameterized

from cbus.common import add_cbus_checksum
from cbus.daemon import cmqttd


def inbound_packet(sals, source=46):
    """Simulate a checksummed Application 56 PCI frame, including source unit."""
    data = bytes([0x05, source, 0x38, 0x00]) + sals
    return add_cbus_checksum(data).hex().upper().encode('ascii') + b'\r\n'


class PublicationMixin:
    def publications(self, events=None):
        result = []
        for call in self.publish_mock.call_args_list:
            _, topic, payload, *options = call.args
            qos = call.kwargs.get('qos', options[0] if options else 0)
            retain = call.kwargs.get(
                'retain', options[1] if len(options) > 1 else False)
            if events is not None and topic.startswith('cbus/events/') != events:
                continue
            try:
                payload = json.loads(payload)
            except ValueError:
                pass  # Switch and binary sensor state payloads are plain text.
            result.append((topic, payload, qos, retain))
        return result

    def make_bridge(self, enabled=True, device_types=None):
        handler = cmqttd.CBusHandler(
            labels=None, device_types=device_types, lighting_events=enabled)
        client = cmqttd.MqttClient(userdata=handler)
        handler.mqtt_api = client
        return handler, client

    def mock_publish(self):
        # Paho 1.5 otherwise creates an internal loopback TCP socket pair even
        # without connect(). Keep the entire test suite free of network sockets.
        socketpair = patch.object(cmqttd.mqtt, '_socketpair_compat',
                                  return_value=(Mock(), Mock()))
        socketpair.start()
        self.addCleanup(socketpair.stop)
        publisher = patch.object(cmqttd.mqtt.Client, 'publish', autospec=True)
        self.publish_mock = publisher.start()
        self.addCleanup(publisher.stop)


class LightingEventsTest(PublicationMixin, unittest.TestCase):
    def setUp(self):
        self.mock_publish()
        self.handler, self.client = self.make_bridge()

    @parameterized.expand([
        ('on', b'\x79\xc8', {}),
        ('off', b'\x01\xc8', {}),
        ('ramp', b'\x0a\xc8\x7f', {'level': 127, 'duration': 4}),
        ('terminate_ramp', b'\x09\xc8', {}),
    ])
    def test_decoded_command(self, event, sals, extra):
        self.handler.data_received(inbound_packet(sals))
        self.assertEqual(self.publications(events=True), [
            ('cbus/events/lighting/200', {
                'event': event, 'source_unit': 46, 'group': 200,
                'application': 56, **extra,
            }, 1, False),
        ])
        states = self.publications(events=False)
        if event == 'terminate_ramp':
            self.assertEqual(states, [])
        else:
            self.assertEqual(len(states), 1)
            self.assertEqual(states[0][0], 'homeassistant/light/cbus_200/state')
            self.assertEqual(states[0][2:], (1, True))

    @parameterized.expand([(46, 0), (1, 200), (46, 255), (255, 1)])
    def test_source_and_group_addresses(self, source, group):
        self.handler.data_received(inbound_packet(bytes([0x09, group]), source))
        self.assertEqual(self.publications(), [
            (f'cbus/events/lighting/{group}', {
                'event': 'terminate_ramp', 'source_unit': source,
                'group': group, 'application': 56,
            }, 1, False),
        ])

    def test_source_zero_keeps_existing_decoder_behavior(self):
        # The unchanged decoder reports source 0 as None. Do not fabricate a
        # source or emit an event; normal state still uses the existing null.
        commands = [b'\x79\xc8', b'\x01\xc8', b'\x0a\xc8\x7f', b'\x09\xc8']
        for enabled in (False, True):
            handler, _ = self.make_bridge(enabled)
            for sals in commands:
                with self.subTest(enabled=enabled, sals=sals):
                    self.publish_mock.reset_mock()
                    handler.data_received(inbound_packet(sals, source=0))
                    self.assertEqual(self.publications(events=True), [])
                    states = self.publications(events=False)
                    if sals[0] == 0x09:
                        self.assertEqual(states, [])
                    else:
                        self.assertEqual(len(states), 1)
                        self.assertIsNone(states[0][1]['cbus_source_addr'])

    @parameterized.expand([
        (0x02, 0, 0), (0x0a, 4, 127), (0x12, 8, 255),
        (0x1a, 12, 127), (0x22, 20, 127), (0x2a, 30, 127),
        (0x32, 40, 127), (0x3a, 60, 127), (0x42, 90, 127),
        (0x4a, 120, 127), (0x52, 180, 127), (0x5a, 300, 127),
        (0x62, 420, 127), (0x6a, 600, 127), (0x72, 900, 127),
        (0x7a, 1020, 42),
    ])
    def test_actual_decoded_ramp_values_in_seconds(self, command, duration, level):
        self.handler.data_received(inbound_packet(bytes([command, 200, level])))
        payload = self.publications(events=True)[0][1]
        self.assertEqual(payload['event'], 'ramp')
        self.assertEqual(payload['level'], level)
        self.assertEqual(payload['duration'], duration)
        self.assertIsInstance(payload['duration'], int)
        self.assertEqual(self.publications(events=False)[0][1]['transition'], duration)

    def test_multiple_groups_and_consecutive_ramp_terminate_sequences(self):
        # Multiple SALs in one packet, followed by another packet; preserve order.
        self.handler.data_received(inbound_packet(b'\x0a\xc8\xff\x09\xc8'))
        self.handler.data_received(inbound_packet(b'\x12\xc9\x00', source=47))
        self.handler.data_received(inbound_packet(b'\x09\xc9', source=47))
        self.handler.data_received(inbound_packet(b'\x02\xc8\x7f\x09\xc8'))
        events = [p[1] for p in self.publications(events=True)]
        self.assertEqual([e['event'] for e in events], [
            'ramp', 'terminate_ramp', 'ramp', 'terminate_ramp',
            'ramp', 'terminate_ramp'])
        self.assertEqual([e['group'] for e in events], [200, 200, 201, 201, 200, 200])
        self.assertEqual([e['source_unit'] for e in events], [46, 46, 47, 47, 46, 46])
        for _, payload, qos, retain in self.publications(events=True):
            self.assertEqual(qos, 1)
            self.assertIs(retain, False)
            if payload['event'] == 'terminate_ramp':
                self.assertEqual(set(payload), {'event', 'source_unit', 'group', 'application'})

    def test_disabled_by_default(self):
        handler = cmqttd.CBusHandler(labels=None)
        handler.mqtt_api = self.client
        self.assertFalse(handler.lighting_events)
        handler.data_received(inbound_packet(b'\x79\xc8\x01\xc8\x0a\xc8\x7f\x09\xc8'))
        self.assertEqual(self.publications(events=True), [])
        self.assertEqual(len(self.publications(events=False)), 3)

    def test_virtual_ignored_group_has_events_without_entities_or_state(self):
        handler, client = self.make_bridge(device_types={200: 'ignore'})
        with patch.object(client, 'subscribe'), patch.object(handler, 'start_queue_system'):
            client.on_connect(client, handler, {}, 0)
        discovery = self.publications()
        self.assertEqual(len(discovery), 256)  # Meta-device plus 255 normal groups.
        self.assertFalse(any(p[0].endswith('cbus_200/config') for p in discovery))
        self.assertEqual(self.publications(events=True), [])
        self.publish_mock.reset_mock()
        handler.data_received(inbound_packet(b'\x79\xc8\x01\xc8\x0a\xc8\x7f\x09\xc8'))
        self.assertEqual([p[1]['event'] for p in self.publications()],
                         ['on', 'off', 'ramp', 'terminate_ramp'])
        self.assertEqual(self.publications(events=False), [])

    @parameterized.expand([
        ('light',), ('light_non_dimmable',), ('switch',), ('binary_sensor',), ('ignore',),
    ])
    def test_existing_state_and_discovery_identical_with_feature_enabled(self, device_type):
        snapshots = []
        for enabled in (False, True):
            self.publish_mock.reset_mock()
            handler, client = self.make_bridge(enabled, {200: device_type})
            client.publish_all_lights({}, handler.device_types)
            handler.data_received(inbound_packet(b'\x79\xc8\x01\xc8\x0a\xc8\x7f\x09\xc8'))
            snapshots.append(self.publications(events=False))
        self.assertEqual(snapshots[0], snapshots[1])
        self.assertTrue(all(p[2:] == (1, True) for p in snapshots[0]))

    def test_reconnect_and_state_publishing_never_emit_command_events(self):
        with patch.object(self.client, 'subscribe'), patch.object(self.handler, 'start_queue_system'):
            for _ in range(2):
                self.client.on_connect(self.client, self.handler, {}, 0)
        self.client.lighting_group_on(None, 200)
        self.client.lighting_group_off(None, 200)
        self.client.lighting_group_ramp(None, 200, 4, 127)
        self.assertEqual(self.publications(events=True), [])

    def test_packets_before_mqtt_ready_are_not_replayed_on_connect(self):
        self.handler.mqtt_api = None
        self.handler.data_received(inbound_packet(b'\x79\xc8\x09\xc8'))
        with patch.object(self.client, 'subscribe'), patch.object(self.handler, 'start_queue_system'):
            self.client.on_connect(self.client, self.handler, {}, 0)
        self.assertEqual(self.publications(events=True), [])

    def test_event_failure_does_not_block_existing_state_update(self):
        with patch.object(self.client, 'publish_lighting_event', side_effect=RuntimeError('test')):
            with self.assertLogs(cmqttd.logger, level='ERROR'):
                self.handler.data_received(inbound_packet(b'\x79\xc8'))
        self.assertEqual(len(self.publications(events=False)), 1)

    def test_partial_packet_is_published_only_when_complete(self):
        packet = inbound_packet(b'\x09\xc8')
        self.handler.data_received(packet[:-2])
        self.publish_mock.assert_not_called()
        self.handler.data_received(packet[-2:])
        self.assertEqual(len(self.publications(events=True)), 1)

    def test_non_lighting_or_null_packet_has_no_events(self):
        self.handler.data_received(inbound_packet(b''))
        self.handler.on_mmi(56, b'\x00')
        self.handler.on_clock_update(46, None)
        self.publish_mock.assert_not_called()


class LightingEventsQueueTest(PublicationMixin, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_publish()
        self.handler, self.client = self.make_bridge()
        self.handler._transport = Mock()  # No actual CNI or socket.

    async def flush_callbacks(self):
        # Run the existing scheduled confirmation/enqueue callback on this loop.
        await asyncio.sleep(0)

    @parameterized.expand([
        ('on', {}, b'{"state":"ON"}', 255, 0),
        ('off', {}, b'{"state":"OFF"}', 0, 0),
        ('ramp', {'duration': 4, 'level': 127},
         b'{"state":"ON","brightness":127,"transition":4}', 127, 4),
    ])
    async def test_ha_request_send_and_confirmation_do_not_emit_events(
            self, command, params, payload, brightness, transition):
        msg = cmqttd.mqtt.MQTTMessage(topic=b'homeassistant/light/cbus_200/set')
        msg.payload = payload
        self.client.on_message(self.client, self.handler, msg)
        await self.flush_callbacks()
        queued = self.handler.command_queue.get_nowait()
        self.assertEqual((queued.command_type, queued.group_addr, queued.params),
                         (command, 200, params))
        code = self.handler._send_queued_command(queued)
        self.handler._transport.write.assert_called_once()
        self.publish_mock.assert_not_called()
        self.handler.pending_confirmations[code] = queued
        self.handler.data_received(code + b'.\r\n')
        await self.flush_callbacks()
        self.assertEqual(self.handler.pending_confirmations, {})
        self.assertEqual(self.publications(events=True), [])
        self.assertEqual(self.publications(events=False), [
            ('homeassistant/light/cbus_200/state', {
                'state': 'OFF' if brightness == 0 else 'ON',
                'brightness': brightness, 'transition': transition,
                'cbus_source_addr': None, 'color_mode': 'brightness',
            }, 1, True),
        ])

    async def test_failure_confirmation_retries_without_event_or_state(self):
        await self.handler.queue_command('on', 200, 'light', {}, {'state': 'ON'})
        queued = self.handler.command_queue.get_nowait()
        self.handler.pending_confirmations[b'g'] = queued
        self.handler.on_confirmation(b'g', False)
        await self.flush_callbacks()
        self.assertEqual(self.handler.pending_confirmations, {})
        self.assertEqual(list(self.handler.retry_queue), [queued])
        self.assertEqual(queued.retry_count, 1)
        self.assertTrue(queued.is_retry)
        self.publish_mock.assert_not_called()

    async def test_timeout_retry_without_event_or_state(self):
        queued = cmqttd.QueuedCommand('on', 200, 'light', timestamp=1)
        self.handler.pending_confirmations[b'g'] = queued
        self.handler._queue_running = True

        async def one_iteration(_delay):
            self.handler._queue_running = False

        with patch.object(cmqttd, 'sleep', side_effect=one_iteration), \
                patch.object(cmqttd.time, 'time', return_value=2):
            await self.handler._timeout_watchdog()
        self.assertEqual(list(self.handler.retry_queue), [queued])
        self.assertEqual(queued.retry_count, 1)
        self.assertEqual(self.handler.pending_confirmations, {})
        self.publish_mock.assert_not_called()

    async def test_terminate_does_not_touch_pending_confirmations(self):
        queued = cmqttd.QueuedCommand('ramp', 200, 'light', params={'duration': 4, 'level': 255})
        self.handler.pending_confirmations[b'g'] = queued
        self.handler.data_received(inbound_packet(b'\x09\xc8'))
        await self.flush_callbacks()
        self.assertEqual(self.handler.pending_confirmations, {b'g': queued})
        self.assertEqual(self.publications(events=False), [])
        self.assertEqual(self.publications(events=True)[0][1]['event'], 'terminate_ramp')

    async def test_observed_command_echo_is_an_event(self):
        # Sending has no synthetic event; a subsequently received bus frame does.
        self.handler.lighting_group_on(200)
        self.publish_mock.assert_not_called()
        self.handler.data_received(inbound_packet(b'\x79\xc8', source=99))
        self.assertEqual(self.publications(events=True)[0][1]['source_unit'], 99)

    @parameterized.expand([(False,), (True,)])
    async def test_cli_flag_reaches_handler_without_network(self, enabled):
        handlers = []
        loop = asyncio.get_running_loop()

        async def connect(factory, _host, _port):
            handler = factory()
            handlers.append(handler)
            handler._connection_lost_future.set_result(True)
            return Mock(), handler

        argv = ['cmqttd', '--broker-address', 'example.invalid',
                '--broker-disable-tls', '--tcp', '192.0.2.1:10001']
        if enabled:
            argv.append('--lighting-events')
        with patch('sys.argv', argv), \
                patch.object(loop, 'create_connection', side_effect=connect), \
                patch.object(cmqttd, 'MqttClient'), \
                patch.object(cmqttd, 'AsyncioHelper'), \
                patch.object(cmqttd.logging, 'basicConfig'):
            await cmqttd._main()
        self.assertEqual(len(handlers), 1)
        self.assertEqual(handlers[0].lighting_events, enabled)
