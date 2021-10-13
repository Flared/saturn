from argparse import Namespace


class ConfigService:
    def __init__(self) -> None:
        self.rabbitmq = Namespace(url="amqp://guest:guest@127.0.0.1/")
