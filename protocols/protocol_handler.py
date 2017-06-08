from lib.common import Node
import logging
import asyncio

from lib.message_queue import ExchangePublisherRabbitManager

class ProtocolHandler(Node):
	"""
	Процесс с подключениями устройств.
	От Node наследую ради working_tick
	"""

	host = '127.0.0.1'
	port = 11111
	protocol = None

	def __init__(self):
		super(ProtocolHandler, self).__init__()
		# передаём protocol_handler, чтобы протоколы могли передавать тики о работе
		self.protocol.protocol_manager = self

	def run(self):
		# запускаем цикл событий

		self.protocol.rabbit_manager = ExchangePublisherRabbitManager(exchange="android_log_exchange", queues=("android_loader_log",),
															 persistent="protocol",
															 channel_confirm_delivery=False,
															 )

		# автоматически loop создаётся только в главном процессе. В остальных его надо создать явно.
		loop = asyncio.new_event_loop()
		asyncio.set_event_loop(loop)

		coro = loop.create_server(self.protocol, self.host, self.port)
		server = loop.run_until_complete(coro)
		logging.info('Serving on {}'.format(server.sockets[0].getsockname()))

		# обрабатываем, пока не будет нажато  Ctrl+C
		try:
			loop.run_forever()
		except KeyboardInterrupt:
			pass

		# закрываем сервер
		server.close()
		loop.run_until_complete(server.wait_closed())
		loop.close()
