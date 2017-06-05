import logging
from time import time, sleep
from lib.common import Node

from lib.database import Database


class LogDatabaseCleaner(Node):

	CLEANUP_PERIOD = 60*60
	DELETE_OLDER_THAN = 60*60*24*30  # in seconds

	def __init__(self):
		super(LogDatabaseCleaner, self).__init__()
		self.last_cleanup_time = time()
		
	def run(self):
		with Database('logdb') as db:
			cursor = db.cursor()

			query = """DELETE FROM android.logs_android 
WHERE (UNIX_TIMESTAMP() - packet_time) > {};""".format(self.DELETE_OLDER_THAN)

			if (time() - self.last_cleanup_time) > self.CLEANUP_PERIOD:
				logging.info("Performing database cleanup!")
				cursor.execute(query)


			sleep(10)
			self.working_tick(True)
