FROM python:3.8

RUN pip3 install mtv_dl

RUN mkdir /data
VOLUME /data
WORKDIR /data

ENTRYPOINT ["mtv_dl"]
