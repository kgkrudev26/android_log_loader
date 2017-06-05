import asyncio
import logging

from lib.message_queue import ExchangePublisherRabbitManager
#TODO: соединение с устройствами иногда (очень редко) вылетает по timeout и приводит к падению программы. Попробовать отловить (https://mail.google.com/mail/#inbox/15c3e5fa6a5a84f0)


class AbstractProtocol(asyncio.Protocol):
    """
    Абстрактный протокол. Создается на каждое соединение с сервером.
        client_id - идентификатор клиента
        buffer - буфер для хранения данных, пришедших от клиента
        is_auth - клиент аутентифицирован
        transport - объект представляющий соединение с клиентом
    """

    protocol_manager = None

    def __init__(self, rabbit_exchange, rabbit_queues):
        self.client_id = 1
        self.client_id_bytes = self.client_id.to_bytes(1, 'little')
        self.buffer = b''
        self.is_auth = True
        self.transport = None

        self.rabbit_manager = ExchangePublisherRabbitManager(exchange=rabbit_exchange, queues=rabbit_queues,
                                                             persistent="protocol",
                                                             channel_confirm_delivery=True,
                                                             )

    def freeze_tick(self):
        """
        Отправляет менеджеру зависаний сообщение о функционировании.
        Это надо вызывать в конце каждого цикла отправки
        :return: 
        """
        self.protocol_manager.working_tick(True)

    def send_to_device(self, command):
        """
        отправляет команду устройству
        :param command: 
        :return: 
        """
        self.transport.write(command.encode())

    def connection_made(self, transport):
        """
        Вызывается после установки соединения.
        :param transport: соединение с клиентом
        :return:
        """
        logging.info("Установлено соединение с новым устройством!")
        self.transport = transport

    def process_data(self, data):
        """
        Проводим манипуляции с данными и отправляем дальше. В случае успеха вернуть True!
        :param data: 
        :return: True или False. Удачно или нет проведена обработка. Нужно для freeze_tick()
        """

        raise Exception("Метод не реализован!")

    def data_received(self, data):
        """
        Вызывается, когда приходят какие-нибудь данные от клиента.
        DO NOT OVERRIDE!!! Override process_data() instead.
        :param data: "какие-нибудь данные"
        :return:
        """
        status = self.process_data(data)
        if status:
            self.freeze_tick()  # менеджеру зависаний

    def connection_lost(self, exc):
        """
        Вызывается, когда теряется соединение с клиентом.
        :param exc: возможное исключение
        :return:
        """
        logging.info("Соединение с клиентом {} потеряно!".format(self.client_id))
