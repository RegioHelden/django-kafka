from django.db import models


class Order(models.Model):
    def __str__(self):
        return f"Order #{self.pk}"
