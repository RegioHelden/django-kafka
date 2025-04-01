FROM python:3.12

ENV PYTHONUNBUFFERED 1
ENV LC_ALL=C.UTF-8

USER root

WORKDIR /app

RUN apt -y update && \
    apt -y --no-install-recommends install gettext && \
    apt clean && \
    find /usr/share/man /usr/share/locale /usr/share/doc -type f -delete && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -m app

USER app

ADD --chown=app ./example/requirements.txt /app/example/
ADD --chown=app ./example/requirements-ci.txt /app/example/

ENV PATH /home/app/venv/bin:$PATH

RUN python3 -m venv ~/venv && \
    pip install -r ./example/requirements.txt

ADD . /app/

ENV DJANGO_SETTINGS_MODULE conf.settings
