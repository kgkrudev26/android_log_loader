import logging
import os
from queue import Queue, Empty
from threading import Thread, Lock
from time import sleep

import pika
from pika.exceptions import ConnectionClosed, ChannelClosed

from config import RABBIT_MQ_CONFIG


class FunctionalityError(Exception):
    pass


class AbstractRabbitManager(Thread):
    """
    
    """

    DEBUG = False

    persistent_connections = dict()

    _lock = Lock()

    def __init__(self, persistent=None, channel_confirm_delivery=False):
        """
        
        :param persistent: если не None, сохраняет глобальное соединение под заданным здесь именем.
        Удобно если потоков много (например, протоколы), и неэффективно создавать соединение для каждого.
        :param channel_confirm_delivery: следует ли каналам задавать режим подтверждения отправки.
        Это необходимо для избежания потерь, но говорят, что он слегка замедляет работу очереди.
        Желательно в компонентах, которые получают данные непосредственно от других систем и не могут 
        слать подтверждения о получении (т.к. у этих систем подтверждений не предусмотрено)
        """
        super(AbstractRabbitManager, self).__init__()
        self.persistent = persistent
        self.channel_confirm_delivery = channel_confirm_delivery

        self.initialize()

    def initialize(self):
        """
        :return: 
        """
        if self.persistent is not None:
            with self._lock:
                try:
                    if not self.persistent_connections[self.persistent].is_open:
                        # если соединение закрылось, переоткрываем и сохраняем
                        self.persistent_connections[self.persistent] = self.connect()
                        self._conn = self.persistent_connections[self.persistent]
                    else:
                        # если соединение есть в словаре и оно открыто, берём его оттуда
                        self._conn = self.persistent_connections[self.persistent]
                except KeyError:
                    # если соединение не создавалось, создаём и сохраняем
                    self._conn = self.persistent_connections[self.persistent] = self.connect()

        else:
            self._conn = self.connect()

        self._channel = self.channel()
        self.enable_confirm_delivery()

    def enable_confirm_delivery(self):
        """
        Включает на канале режим, удостоверяющийся, что пакеты доходят.
        Также надо выставить delivery_mode=2. Тогда протоколы не теряют сообщения.
        :return: 
        """
        if self.channel_confirm_delivery:
            self._channel.confirm_delivery()

    def connect(self):
        """
        Создаём соединение с RabbitMQ
        :return: 
        """
        credentials = pika.credentials.PlainCredentials(RABBIT_MQ_CONFIG['user'], RABBIT_MQ_CONFIG['passwd'])
        parameters = pika.ConnectionParameters(host=RABBIT_MQ_CONFIG['host'], port=RABBIT_MQ_CONFIG['port'],
                                               credentials=credentials)
        connection = pika.BlockingConnection(parameters)

        return connection

    def channel(self):
        return self._conn.channel()

    def declare_queue(self, queue_name):
        self._channel.queue_declare(queue=queue_name,
                                    auto_delete=False,
                                    )

    def catch_disconnect(self, f, *args, **kwargs):
        while True:
            try:
                result = f(*args, **kwargs)
                break
            except (BrokenPipeError, ConnectionClosed, ChannelClosed):
                # бывает, что ошибка возникает по нескольку раз.
                # На НЕперсистентных соединениях могут возникать дублирующиеся соединения без каналов (не знаю, почему)
                # это не страшно, дохлые безканальные соединения закрываются сами через несколько минут
                logging.critical("Соединение с RabbitMQ потеряно! Перезапускаю процесс!")
                os._exit(1)
                sleep(1)
            except AttributeError:
                # та самая "странная" ошибка
                logging.critical("Неизвестная ошибка (скорее всего связанная с глюком многопоточности "
                                 "при работе с pika)! "
                                 "Убиваю процесс!")
                os._exit(1)
                sleep(1)
            except KeyboardInterrupt:
                logging.warning("Завершаю чтение очереди")

        return result


class ExchangePublisherRabbitManager(AbstractRabbitManager):
    """
    Менеджер очередей RabbitMQ, способный только публиковать данные через обменника.
    Но не считывать и не отправлять непосредственно в очередь.
    """
    def __init__(self, exchange, queues, persistent=None, channel_confirm_delivery=False):
        """
        
        :param exchange: имя обменника.
        :param queues: список имён очередей, которые надо привязать к обменнику.
        Используется только для декларирования очередей и привязки их к обменнику.
        :param persistent: 
        """
        super(ExchangePublisherRabbitManager, self).__init__(persistent=persistent,
                                                             channel_confirm_delivery=channel_confirm_delivery,
                                                             )
        self.exchange_name = exchange + ("_test" if self.DEBUG else "")
        # кортеж имён очередей
        self.queue_names = tuple(q + ("_test" if self.DEBUG else "") for q in queues)

        self.declare_exchange(self.exchange_name)
        self.declare_and_bind_queues(self.exchange_name, self.queue_names)

    def declare_exchange(self, exchange_name):
        self._channel.exchange_declare(exchange=exchange_name,
                                       type='fanout',
                                       auto_delete=False,
                                       )

    def bind_queue(self, exchange_name, queue_name):
        """Привязывает очередь к обменнику"""
        self._channel.queue_bind(exchange=exchange_name, queue=queue_name)

    def declare_and_bind_queues(self, exchange_name, queue_names):
        """Создаёт очереди и привязывает их к обменнику"""
        for q in queue_names:
            self.declare_queue(q)
            self.bind_queue(exchange_name, q)

    def _send_message(self, message):
        self._channel.basic_publish(exchange=self.exchange_name,
                                    body=message,
                                    routing_key='',
                                    properties=pika.BasicProperties(delivery_mode=2),
                                    )

    def send_message(self, message):
        self.catch_disconnect(self._send_message, message)

    def run(self):
        pass


class QueueSenderRabbitManager(AbstractRabbitManager):
    """
    Менеджер очередей RabbitMQ, способный отправлять данные только непосредственно в очередь,
    но не через exchange, и не считывать.
    """
    def __init__(self, queue, persistent=None):
        super(QueueSenderRabbitManager, self).__init__(persistent=persistent,
                                                       )
        self.queue_name = queue + ("_test" if self.DEBUG else "")

        self.declare_queue(self.queue_name)

    def _send_message(self, message):
        self._channel.basic_publish(exchange='',
                                    body=message,
                                    routing_key=self.queue_name)

    def send_message(self, message):
        self.catch_disconnect(self._send_message, message)

    def run(self):
        pass


class ReaderRabbitManager(AbstractRabbitManager):
    """
    Менеджер очередей RabbitMQ, способный только считывать данные,
    но не отправлять.
    """
    def __init__(self, queue, persistent=None, autostart=True, auto_ack=False):
        super(ReaderRabbitManager, self).__init__(persistent=persistent)
        self.queue_name = queue + ("_test" if self.DEBUG else "")
        self.local_queue = Queue(maxsize=1000)
        self.auto_ack = auto_ack

        if autostart:
            self.start_queue_reading()

    def start_queue_reading(self):
        """
        Запускает поток, обрабатывающий очередь
        :return: 
        """
        self.start()

    def run(self):
        """
        run() потока чтения очереди
        :return: 
        """
        self.consume_queue()

    def _consume_queue(self):
        self._channel.basic_consume(self._queue_callback, queue=self.queue_name)
        self._channel.start_consuming()

    def consume_queue(self):
        self.catch_disconnect(self._consume_queue)

    def _queue_callback(self, ch, method, properties, body):
        """
        при получении данных из RabbitMQ кладём их в локальную очередь
        :param ch: 
        :param method: 
        :param properties: 
        :param body: 
        :return: 
        """
        self.local_queue.put((ch, method, properties, body,))

        if self.auto_ack:
            self.ack(tag=method.delivery_tag)

    def ack(self, tag, channel=None, multiple=True):
        self.catch_disconnect(self._ack, tag, channel=channel, multiple=multiple)

    def _ack(self, tag, channel=None, multiple=True):
        if not channel:
            channel = self._channel
        channel.basic_ack(tag, multiple=multiple)

    def read_one(self, block=False):
        """
        Прочитать один блок данных из локальной очереди.
        :param block: 
        :return: 
        """
        result = self.local_queue.get(block=block)
        return result

    def read_all(self, block=False):
        """
        Прочитать все блоки данных и выдать листом
        :return: 
        """
        result = []
        while True:
            try:
                bl = self.local_queue.get(block=block)
                result.append(bl)
            except Empty:
                break
        return result
