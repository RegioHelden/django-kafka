FROM python:3.12

ENV PYTHONUNBUFFERED 1
ENV LC_ALL=C.UTF-8

RUN useradd -m app

USER app
WORKDIR /app

ADD ./example/requirements.txt /app/example/

ENV PATH /home/app/venv/bin:$PATH

RUN python3 -m venv ~/venv && \
    pip install -r ./example/requirements.txt

ADD . /app/

ENV DJANGO_SETTINGS_MODULE conf.settings
