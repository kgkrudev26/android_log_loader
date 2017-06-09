# Чинит зависание других процессов из-за бага в logging http://bugs.python.org/issue6721
# Взято с https://github.com/google/python-atfork
# Нужно проворачивать перед всем остальным, даже перед import'ами
from lib.atfork import monkeypatch_os_fork_functions
from lib.atfork.stdlib_fixer import fix_logging_module
fix_logging_module()
monkeypatch_os_fork_functions()

import logging
import argparse

from lib.message_queue import AbstractRabbitManager
from lib.aliver import ProcessAliver

# парсим аргументы командной строки
parser = argparse.ArgumentParser()
parser.add_argument('host')
parser.add_argument('port')
parser.add_argument('--loglevel', choices=['DEBUG', 'INFO', 'WARNING'])
parser.add_argument('--test-queue', action='store_true', dest='test_queue',
					help="Создаёт тестовые очереди и обменники RabbitMQ "
						"(с постфиксом _test) и использует их.")
args = parser.parse_args()
host = args.host
port = args.port

ProcessAliver.port = port

# настройки лога
numeric_level = getattr(logging, args.loglevel)
logging.basicConfig(format=u'[%(asctime)s] %(pathname)s:%(filename)s[LINE:%(lineno)d]# %(levelname)-8s  %(message)s',
					level=numeric_level)

AbstractRabbitManager.DEBUG = args.test_queue

# настраиваем протоколы
from protocols.android_log_protocol import AndroidLogProtocol
protocol = AndroidLogProtocol

from protocols.protocol_handler import ProtocolHandler
protocol_handler = ProtocolHandler
protocol_handler.protocol = protocol
protocol_handler.host = host
protocol_handler.port = port

from loaders.logdb_loader import AndroidLogLoader
log_packet_loader = AndroidLogLoader

# from cleaners.log_base_cleaner import LogDatabaseCleaner
# db_cleaner = LogDatabaseCleaner
db_cleaner = None

# собираем классы доступных компонентов системы в кортеж
component_classes = tuple(filter(None, (protocol_handler, log_packet_loader, db_cleaner)))
print("component_classes", component_classes)#debug
aliver = ProcessAliver(component_classes)
aliver.join()
