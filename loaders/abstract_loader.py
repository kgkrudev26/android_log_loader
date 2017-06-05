import logging
from time import time, sleep
from queue import Empty

from lib.common import Node
from lib.message_queue import ReaderRabbitManager
# from lib.database import Database


class AbstractLoader(Node):
	"""
	
	"""
	def __init__(self, protocol_name, loader_type):
		"""
		
		:param protocol_name: имя протокола
		:param loader_type: имя типа загрузчика
		"""
		super(AbstractLoader, self).__init__()

		# Максимальное количество блоков данных, которое можно залить в базу за один запрос.
		self.MAX_DATABLOCKS_PER_QUERY = 100
		self.BUFFER_FLUSH_TIMEOUT = 1  # сливаем буффер в базу через это время
		self.last_flush_time = time()
		self.buffer = []  # сюда пишутся обработанные данные для последующего группового слива в базу
		self.db_name = ''  # имя базы, как прописано в config.py
		self.table = ''  # имя таблицы для записи пакета
		self.fields = ()  # поля таблицы,
		self.query = None  # запрос в базу для слива данных

		self.rabbit_queue_name = "_".join((protocol_name, "loader", loader_type))
		logging.info("loader queue {}".format(self.rabbit_queue_name))#debug

	def run(self):
		# нужно задать менеджер здесь, иначе локальная очередь окажется в разных процессах
		self.read_rabbit_manager = ReaderRabbitManager(queue=self.rabbit_queue_name,
														autostart=True,
														auto_ack=False,)
		super(AbstractLoader, self).run()

	# def flush(self):
	# 	"""
	# 	Сливает буффер в базу
	# 	:return:
	# 	"""
	#
	# 	with Database(self.db_name, persistent=True) as db:
	# 		cursor = db.cursor()
	# 		while self.buffer:
	# 			data_to_flush = self.buffer[:self.MAX_DATABLOCKS_PER_QUERY]
	# 			self.buffer = self.buffer[self.MAX_DATABLOCKS_PER_QUERY:]
	# 			# logging.info("rows flushed: {}".format(len(data_to_flush)))#debug
	# 			# logging.info("query: {}".format(self.query))#debug
	# 			rows_inserted = cursor.executemany(self.query, data_to_flush)
	# 			# logging.info("rows_inserted {}".format(rows_inserted))#debug
	# 			self.working_tick(self.ack(True))  # отправляем подтверждение успешной обработки пакета

	def flush(self):
		"""
		Тестовое, пока базу не наладили.
		:return: 
		"""

		while self.buffer:
			data_to_flush = self.buffer[:self.MAX_DATABLOCKS_PER_QUERY]
			self.buffer = self.buffer[self.MAX_DATABLOCKS_PER_QUERY:]
			logging.info("flushing with {} \n {}".format(self.query, data_to_flush))
			self.working_tick(self.ack(True))  # отправляем подтверждение успешной обработки пакета

	def check_flush(self):
		"""
		Проверяет, не пора ли слить буфер в базу.
		:return: 
		"""
		time_since_flush = time() - self.last_flush_time
		# буфер не пуст и время пришло
		if self.buffer and time_since_flush > self.BUFFER_FLUSH_TIMEOUT:
			# logging.info("flushing! Buffer: {}".format(self.buffer))#debug
			self.flush()
			self.buffer.clear()#debug!!!
			self.last_flush_time = time()

	def get(self, block=False):
		while True:
			self.check_flush()
			try:
				# получаем данные (служебная инфа RabbitMQ записывается в переменные self.*)
				data = super(AbstractLoader, self).get(block=False)
				break
			except Empty:
				sleep(1)

		return data

	def put(self, data):
		self.buffer.append(data)
		# пока не отправляем подтверждение. Слив в базу происходит во flush
		return False


class AbstractLogLoader(AbstractLoader):
	"""docstring for AbstractLogLoader"""
	def __init__(self, protocol_name):
		loader_type = "log"
		super(AbstractLogLoader, self).__init__(protocol_name=protocol_name, loader_type=loader_type)
		

