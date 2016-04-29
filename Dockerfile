FROM semtech/mu-python-template:latest

MAINTAINER cecile.tonglet@gmail.com

VOLUME /data

ENV ENV prod
ENV PORT 5000

ADD requirements.txt /app/
WORKDIR /app
RUN pip3 install -r requirements.txt

ADD server.py /app/
ADD mu_semtech /app/mu_semtech

CMD ["./server.py"]
