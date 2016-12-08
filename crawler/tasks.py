from __future__ import absolute_import, unicode_literals
from .celery import app


@app.task
def add(x, y):
    return x + y


def main():
    for i in range(10):
        add.delay(5, 5)
