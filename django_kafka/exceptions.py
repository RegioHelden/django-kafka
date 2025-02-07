from typing import Optional


class DjangoKafkaError(Exception):
    def __init__(self, *args, context: Optional[any] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.context = context
