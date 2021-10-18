from argparse import Namespace


class ConfigService:
    def __init__(self) -> None:
        self.rabbitmq = Namespace(url="amqp://127.0.0.1/")
