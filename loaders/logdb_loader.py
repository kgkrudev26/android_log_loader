import json

from loaders.abstract_loader import AbstractLogLoader


class AndroidLogLoader(AbstractLogLoader):

	protocol_name = "android"

	def __init__(self):
		super(AndroidLogLoader, self).__init__(protocol_name=self.protocol_name)

		self.db_name = 'logdb'
		self.table = 'logs'
		self.fields = ('id', 'time', 'text',)

		self.query = "INSERT IGNORE INTO {table}({fields}) VALUES ({formatting})".format(
			table=self.table, fields=",".join(self.fields), formatting=",".join('%s' for _ in range(len(self.fields)))
		)

	def handle(self, value):
		parse = json.loads(value.decode('utf-8'))

		device_id = int(parse.get("id", 1))
		receive_time = int(parse.get("time", 0))
		text = parse.get("text", "")

		return device_id, receive_time, text