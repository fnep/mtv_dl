FROM python:3.10 AS build
RUN pip3 install poetry
COPY ./ /app
WORKDIR /app
RUN poetry install
RUN poetry build
RUN rm -f dist/mtv_dl-0.0.0-py3-none-any.whl

FROM python:3.10
COPY --from=build /app/dist/*.whl /tmp/
RUN pip3 install /tmp/*.whl && rm /tmp/*.whl
RUN mkdir /data
VOLUME /data
WORKDIR /data
ENTRYPOINT ["mtv_dl"]
