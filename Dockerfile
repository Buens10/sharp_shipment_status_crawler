FROM jupyter/base-notebook

RUN mkdir -p /usr/src
WORKDIR /usr/src

RUN pip install --upgrade pip
COPY ./requirements.txt /usr/src/requirements.txt

RUN pip install -r requirements.txt

COPY . /usr/src

ENV PYTHONPATH /usr/src


