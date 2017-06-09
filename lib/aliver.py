import logging
from time import sleep, time
from threading import Thread
from multiprocessing import Queue as mqueue
import subprocess

class ProcessAliver(Thread):
	"""
	Берёт классы компонентов (которые должны наследоваться от multiprocessing.Process),
	запускает и следит за их выполнением.
	Если процесс компонента упадёт, он будет перезапущен.
	Если процесс зависнет (т.е. не будет присылать подтверждение работы в соответствующую очередь из tick_queues),
	он будет через определённое время (заданное в MAX_FROZEN_TIME) убит и перезапущен.
	"""

	# максимальное время, которое процесс может не откликаться. Выше этого - он будет убит.
	MAX_FROZEN_TIME = 30

	port = 8000  # порт, куда подсоединяются устройства

	def __init__(self, components):
		"""
		
		:param components: классы (не экземпляры!) компонентов системы, наследующие multiprocessing.Process
		"""
		super(ProcessAliver, self).__init__()
		self.component_classes = components

		# здесь будут потоки, следящие за процессами
		self.handler_threads = [None]*len(self.component_classes)
		# собственно процессы
		self.component_processes = [None]*len(self.component_classes)
		# время старта процесса или получения последнего сигнала о работе
		self.freezekill_start_times = [0]*len(self.component_classes)
		# сюда приходят сигналы о том, что процесс всё ещё работает
		self.tick_queues = [None]*len(self.component_classes)

		self.start()

	def run(self):
		while True:
			for n, component in enumerate(self.component_classes):
				# проверяем поток слежения
				if not self.handler_threads[n] or not self.handler_threads[n].is_alive():
					t = Thread(target=self.process_launcher, args=(component, n,))
					t.start()
					self.handler_threads[n] = t

				# проверяем, пришёл ли сигнал о работе процесса
				if self.tick_queues[n] and not self.tick_queues[n].empty():
					# очищаем очередь
					while not self.tick_queues[n].empty():
						self.tick_queues[n].get()
					self.freezekill_start_times[n] = time()  # выставляем таймер

				# проверяем, не завис ли процесс
				if self.component_processes[n] and (time() - self.freezekill_start_times[n]) > self.MAX_FROZEN_TIME:
					logging.warning("Process {} appears to be frozen! Killing!".format(self.component_processes[n]))
					self.component_processes[n].terminate()

				# Соединения с устройствами на уровне сервера почему-то не закрываются.
				# В результате они накапливаются, программа упирается в лимит и выдаёт ошибку
				# OSError: [Errno 24] Too many open files
				# Значит будем отслеживать число соединений на этом порту и убивать приёмщик, когда их накопится много.
				MAX_OPEN_CONNS = 500
				if component.__name__ == "ProtocolHandler":
					ps = subprocess.Popen(['lsof', '-i', 'TCP:{}'.format(self.port)], stdout=subprocess.PIPE)
					out = ps.communicate()[0]
					open_conns = len(out.decode().split('\n'))
					logging.info("Open connections: {}".format(open_conns))
					if open_conns > MAX_OPEN_CONNS:
						logging.warning("Receiver has opened more than {} connections. Restarting!".format(MAX_OPEN_CONNS))
						self.component_processes[n].terminate()


			sleep(5)

	def process_launcher(self, proc_class, proc_index):
		while True:
			p = proc_class()

			# задаём очередь, через которую будет сообщаться о том, что процесс всё ещё на ходу
			tick_queue = mqueue()
			p.tick_queue = tick_queue  # присваеваем процессу
			self.tick_queues[proc_index] = tick_queue  # сохраняем
			self.freezekill_start_times[proc_index] = time()  # сбрасываем таймер зависаний

			self.component_processes[proc_index] = p  # сохраняем handle на процесс
			p.start()
			p.join()
			sleep(1)
