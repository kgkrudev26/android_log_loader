from multiprocessing import Process


def int_from_bits(data, count_bits, start_bit=0):
    return (int.from_bytes(data, 'little') >> start_bit) % (1 << count_bits)


def get_bit(src, bit):
    """Возвращает бит под номером `bit` (справа)"""
    if isinstance(src, bytes):
        src = int.from_bytes(src, 'little')
    return (src >> bit) & 1


def set_bit(src, bit, val):
    """Возвращает src, в котором бит под номером `bit` (справа) выставлен на value"""
    if isinstance(src, bytes):
        src = int.from_bytes(src, 'little')
    if val:
        return src | (1 << bit)
    else:
        return src & ~(1 << bit)


class Node(Process):
    """
    Абстрактный класс представляющий из себя некий вычислительный узел.
    Метод get - возвращает очередное значение, которое пришло на обработку.
    Метод validate - проверяет пакет на правильность. Возвращает None в случае неправильности.
    Тогда пакет сбрасывается.
    Метод handle - обрабатывает его.
    Метод put - получает результат работы метода handle и что-то с ним делает.
    Это может быть передача результата на следующий вычислительный узел, а может быть сохранение в БД.
    Метод ack - отправляет подтверждение получения сообщения очереди RabbitMQ, если статус True
    Метод working_tick - отправляет уведомление запускателю о том, что он всё ещё работает
    Не существует каких-либо ограничений на то, что делать этому методу с результатом обработки значения.
    """

    def __init__(self):
        super(Node, self).__init__()
        self.read_rabbit_manager = None  # менеджер чтения очереди RabbitMQ (обычно ReaderRabbitManager)
        # Очередь для сигналов менеджеру зависаний. multiprocessing.Queue. Задаём в инициализации подклассов.
        self.tick_queue = None
        # служебная информация RabbitMQ
        self.current_ch, self.current_method, self.current_properties = None, None, None

    def run(self):
        while True:
            data = self.validate(self.get())
            if data:
                # данные валидны
                self.working_tick(self.ack(self.put(self.handle(data))))
            else:
                # данные не валидны, validate() вернул None. Сбрасываем пакет
                self.working_tick(self.ack(status=True, multiple=False))

    def get(self, block=True):
        """
        Получаем данные из очереди с помощью self.read_rabbit_manager. Данные возвращаем.
        Служебную информацию сохраняем в переменные instance'а.
        :return: сырые данные
        """
        self.current_ch, self.current_method, self.current_properties, body = self.read_rabbit_manager.read_one(
            block=block)
        return body

    def validate(self, data):
        """
        ВНИМАНИЕ! Не использовать в загрузчиках и других компонентах, которые обрабатывают пакеты 
        и отправляют подтверждения не по-одному, а скопом.
        Это приводит к ошибкам с delivery tag (предположительно, из-за попытки двойного подтверждения).
        
        Валидируем данные, что они соответствуют формату компонента.
        Верните None в случае неправильности пакета.
        :param data: 
        :return: 
        """
        return data

    def handle(self, data):
        """
        Обрабатываем данные
        :param data: сырые данные
        :return: обработанные данные
        """
        return data

    def put(self, result):
        """
        
        :param result: 
        :return: True или False: соответственно в случае успеха или провала операции 
        """
        pass

    def ack(self, status, multiple=True, tag=None):
        """
        Подтверждение в RabbitMQ. Заставляет его сбросить обработанные данные.
        :param multiple: 
        :param status: 
        :return: 
        """
        if status:
            _tag = tag if tag else self.get_current_tag()
            self.read_rabbit_manager.ack(tag=_tag, multiple=multiple)
            return True

    def working_tick(self, status):
        if status:
            # отправляем сигнал о работе менеджеру зависаний
            self.tick_queue.put(1)

    def get_current_tag(self):
        """Возвращает номер сообщения очереди RabbitMQ. Нужно для подтверждения получения (ack)."""
        return self.current_method.delivery_tag


class ProtocolException(Exception):
    pass
