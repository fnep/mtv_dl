FROM ghcr.io/astral-sh/uv:python3.13-alpine AS build
COPY ./ /app/
WORKDIR /app
RUN apk add git
RUN uv sync --frozen && uv build

FROM python:3.13
COPY --from=build /app/dist/*.whl /tmp/
RUN pip3 install /tmp/*.whl && rm /tmp/*.whl
RUN mkdir /data
VOLUME /data
WORKDIR /data
ENTRYPOINT ["mtv_dl"]
