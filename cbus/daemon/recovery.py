"""Connection ownership and diagnostics; no lighting-event synthesis."""

import asyncio
import logging
import time


logger = logging.getLogger(__name__)


async def maintain_cbus_connection(factory, connect, mqtt_client,
                                   retry_min=1, retry_max=60,
                                   connect_timeout=30, stable_time=60):
    """One owner, one transport and a fresh protocol for each attempt.

    Cancellation is the shutdown signal. Backoff resets only after a stable
    connection, so interfaces accepting then immediately closing cannot spin.
    """
    delay = retry_min
    while True:
        protocol = factory()
        transport = None
        connected_at = None
        try:
            logger.info('Connecting to C-Bus interface')
            transport, _ = await asyncio.wait_for(
                connect(lambda: protocol), timeout=connect_timeout)
            connected_at = time.monotonic()
            mqtt_client.attach_handler(protocol)
            logger.info('C-Bus transport connected; PCI initialisation sent')
            await protocol._connection_lost_future
            logger.warning('C-Bus transport lost; reconnecting')
        except (OSError, asyncio.TimeoutError) as exc:
            logger.warning('C-Bus connection attempt failed: %s', exc)
        finally:
            mqtt_client.attach_handler(None)
            protocol.mqtt_api = None
            # A timeout/cancellation may occur after connection_made but before
            # create_connection returns its transport. The protocol owns it too.
            active_transport = transport or protocol._transport
            if active_transport is not None:
                active_transport.abort()
            await protocol.shutdown()
        if connected_at is not None and time.monotonic() - connected_at >= stable_time:
            delay = retry_min
        logger.info('Retrying C-Bus connection in %s seconds', delay)
        await asyncio.sleep(delay)
        delay = min(delay * 2, retry_max)


async def log_health(mqtt_client, interval=300):
    """A normal-level heartbeat makes a quiet link distinguishable from no loop."""
    while True:
        await asyncio.sleep(interval)
        handler = mqtt_client.bridge_handler
        rx_age = (None if handler is None or handler.last_rx_time is None else
                  round(time.monotonic() - handler.last_rx_time, 1))
        probe_age = (None if handler is None or handler.last_probe_time is None else
                     round(time.monotonic() - handler.last_probe_time, 1))
        logger.info('Bridge health: MQTT=%s C-Bus=%s subscriptions=%s last_rx_seconds=%s last_probe_ack_seconds=%s queued=%s pending=%s',
                    'connected' if mqtt_client.is_connected() else 'disconnected',
                    'connected' if handler is not None and handler._transport is not None else 'disconnected',
                    mqtt_client.subscriptions_ready, rx_age, probe_age,
                    handler.command_queue.qsize() if handler else 0,
                    len(handler.pending_confirmations) if handler else 0)
