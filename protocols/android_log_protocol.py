import logging
import json
from json.decoder import JSONDecodeError

from protocols.abstract_protocol import AbstractProtocol


class AndroidLogProtocol(AbstractProtocol):

	rabbit_exchange = "android_log_exchange"
	rabbit_queues = ("android_loader_log",)

	def __init__(self):
		super(AndroidLogProtocol, self).__init__(rabbit_exchange=self.rabbit_exchange,
												rabbit_queues=self.rabbit_queues)

	def process_data(self, data):
		try:
			parse = json.loads(data.decode('utf-8'))
			logging.info("Got data {}".format(parse))

			if parse.get('type', None) == "log":
				if self.validate(parse):
					self.put_to_queue(data)
		except JSONDecodeError:
			logging.warning("Got unparseable packet! Dropping!")

		return True

	def validate(self, data):
		"""
		Проверяет, что данные правильные
		:param data: 
		:return: 
		"""
		try:
			if not isinstance(data['id'], int) or len(str(data['id'])) > 10:
				logging.warning("Device ID is not an integer or is too long!")
				return False
			if not (isinstance(data['time'], int) or isinstance(data['time'], float)):
				logging.warning("Time is not numeric!")
				return False
			if not (isinstance(data['text'], str)):
				logging.warning("Log text is not a string!")
				return False

			return True
		except KeyError:
			logging.warning("Fields missing!")
			return False

	def put_to_queue(self, data):
		"""
		Вызывается, чтобы передать данные от приемщика на дальнейшую обработку.
		:param data: сами данные
		:return:
		"""
		self.rabbit_manager.send_message(data)
