FROM python:3.7.2-stretch

WORKDIR /flask

ADD . /flask

RUN cd /flask

RUN pip install -r REQUIREMENTS.txt

CMD ["uwsgi", "app.ini"]