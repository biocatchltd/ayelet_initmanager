import logging
from typing import Any, Callable

import aio_pika
import envolved
from aio_pika import IncomingMessage
from aio_pika.abc import AbstractRobustConnection

logger = logging.getLogger(('biocatch.' + __name__))


class QueueConsumer():

    async def close(self) -> None:
        await self.connection.close()

    async def _create_connection(self) -> AbstractRobustConnection:
        try:
            rabbitmq_port: envolved.EnvVar[str] = \
                envolved.env_var('rabbitmq_port', type=str)
            port = rabbitmq_port.get()
            rabbitmq_host: envolved.EnvVar[str] = \
                envolved.env_var('rabbitmq_host', type=str)
            host = rabbitmq_host.get()
            return await aio_pika.connect_robust(host=host, port=port, timeout=6000)
        except Exception:
            logger.exception(f'rabbit mq connection failed')
            raise

    async def _get_channel(self):
        self.connection = await self._create_connection()
        channel = await self.connection.channel(publisher_confirms=False)
        rabbitmq_exchange: envolved.EnvVar[str] = \
            envolved.env_var('rabbitmq_exchange', type=str)
        self._exchange = rabbitmq_exchange.get()
        channel.default_exchange = await channel.declare_exchange(self._exchange)
        return channel

    async def prepare(self, callback: Callable[[IncomingMessage], Any]) -> None:
        channel = await self._get_channel()
        await channel.set_qos()
        rabbitmq_queue_name: envolved.EnvVar[str] = \
            envolved.env_var('rabbitmq_queue_name', type=str)
        queue_name = rabbitmq_queue_name.get()
        self._queue = await channel.declare_queue(queue_name)
        self._callback = callback

    async def start_consuming(self) -> None:
        try:
            await self._queue.consume(self._callback)
        except Exception:
            logger.exception("Exception raised while consuming from queue")
            raise


async def init_rabbitmq_consumer(callback) -> QueueConsumer:
    consumer = QueueConsumer()
    await consumer.prepare(callback)
    return consumer
