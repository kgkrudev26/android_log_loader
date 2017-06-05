from time import sleep
from multiprocessing import Process

from lib.database import Database


class LogDatabaseCleaner(Process):

	CLEANUP_PERIOD = 60*60
	DELETE_OLDER_THAN = 60*60*24*30  # in seconds

	def __init__(self):
		super(LogDatabaseCleaner, self).__init__()
		
	def run(self):
		with Database('logdb') as db:
			cursor = db.cursor()
			query = """DELETE FROM android.logs_android 
WHERE (UNIX_TIMESTAMP() - packet_time) > {};""".format(self.DELETE_OLDER_THAN)

			cursor.execute(query)
			sleep(self.CLEANUP_PERIOD)
