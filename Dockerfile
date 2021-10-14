###########
## BUILD ##
###########
FROM python:3.10-slim-bullseye as build

ENV POETRY_VERSION=1.1.11
WORKDIR /opt/saturn

RUN pip install poetry==${POETRY_VERSION}

ADD pyproject.toml poetry.lock README.md ./
ADD src ./src

RUN poetry build

################
## MAIN IMAGE ##
################
FROM python:3.10-slim-bullseye

COPY --from=build /opt/saturn/dist/*.whl /opt/saturn/

RUN pip install /opt/saturn/*.whl \
    && rm -rf /opt/saturn

ENV FLASK_HOST=0.0.0.0
ENV FLASK_PORT=80

EXPOSE 80

CMD python -m saturn_engine.worker_manager.server
