class DjangoKafkaError(Exception):
    def __init__(self, *args, context: any = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.context = context
