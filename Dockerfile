FROM python:3.4-wheezy
LABEL authors="Cecile Tonglet <cecile.tonglet@gmail.com>"

ADD https://raw.githubusercontent.com/guilhem/apt-get-install/master/apt-get-install /usr/bin/
RUN chmod +x /usr/bin/apt-get-install

ENV ENV prod
ENV PORT 80
ENV MU_SPARQL_ENDPOINT 'http://database:8890/sparql'
ENV MU_APPLICATION_GRAPH 'http://mu.semte.ch/application'

RUN apt-get-install nginx

ADD requirements.txt /app/
WORKDIR /app
RUN pip3 install -r requirements.txt

RUN rm /etc/nginx/sites-enabled/default
COPY flask.conf /etc/nginx/sites-available/
RUN ln -s /etc/nginx/sites-available/flask.conf /etc/nginx/sites-enabled/flask.conf

ADD flask_restful_sparql /app/flask_restful_sparql
ADD server.py app.ini entrypoint.sh /app/
ADD mu_semtech /app/mu_semtech

VOLUME /data

CMD ["/app/entrypoint.sh"]
