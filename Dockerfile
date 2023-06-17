FROM python:3.10 AS build
RUN pip3 install poetry poetry-dynamic-versioning
COPY ./ /app/
RUN rm -rf /app/dist
WORKDIR /app
RUN poetry install
RUN poetry build

FROM python:3.10
COPY --from=build /app/dist/*.whl /tmp/
RUN pip3 install /tmp/*.whl && rm /tmp/*.whl
RUN mkdir /data
VOLUME /data
WORKDIR /data
ENTRYPOINT ["mtv_dl"]
