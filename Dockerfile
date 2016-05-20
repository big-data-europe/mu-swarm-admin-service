FROM semtech/mu-python-template:latest

MAINTAINER cecile.tonglet@gmail.com

VOLUME /data

RUN apt-get update && apt-get -y install nginx
RUN rm /etc/nginx/sites-enabled/default
COPY flask.conf /etc/nginx/sites-available/
RUN ln -s /etc/nginx/sites-available/flask.conf /etc/nginx/sites-enabled/flask.conf

ENV ENV prod
ENV PORT 80

ADD requirements.txt /app/
WORKDIR /app
RUN pip3 install -r requirements.txt

ADD server.py app.ini entrypoint.sh /app/
ADD mu_semtech /app/mu_semtech

CMD ["/app/entrypoint.sh"]
