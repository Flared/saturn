###########
## BUILD ##
###########
FROM python:3.10-slim-bullseye as build

ENV POETRY_VERSION=1.1.11

RUN apt-get update \
    && apt-get install -y git build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && pip install poetry==${POETRY_VERSION}

WORKDIR /opt/saturn

ADD pyproject.toml poetry.lock README.md ./
ADD src ./src

RUN poetry build

################
## MAIN IMAGE ##
################
FROM python:3.10-slim-bullseye

COPY --from=build /opt/saturn/dist/*.whl /opt/saturn/

RUN apt-get update \
    && apt-get install -y git build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && pip install /opt/saturn/*.whl \
    && rm -rf /opt/saturn

ENV SATURN_FLASK_HOST=0.0.0.0
ENV SATURN_FLASK_PORT=80
ENV SATURN_DATABASE_URL=sqlite:///tmp/saturn.sqlite

EXPOSE 80

CMD python -m saturn_engine.worker_manager.server
