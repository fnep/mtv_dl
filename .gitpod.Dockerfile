FROM gitpod/workspace-full:latest
USER gitpod

RUN python3 -m pip install poetry
RUN python3 -m poetry config virtualenvs.create false