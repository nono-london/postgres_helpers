FROM ubuntu:24.04@sha256:9c7f200c07e3d4a03e7249d2c16bf66a51120fb876aa2242cf31451412543289

SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates software-properties-common postgresql-common \
    && printf '\ncreate_main_cluster = false\n' >> /etc/postgresql-common/createcluster.conf \
    && add-apt-repository --yes ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-cache policy python3.9 \
        | grep --fixed-strings 'ppa.launchpadcontent.net/deadsnakes/ppa' \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        python3.9=3.9.25-1+noble1 \
        python3.9-venv=3.9.25-1+noble1 \
        postgresql-16 postgresql-client-16 \
    && /usr/bin/python3.9 -m venv /opt/venv \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/venv/bin:/usr/lib/postgresql/16/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN useradd --create-home --shell /bin/bash ci \
    && chown -R ci:ci /opt/venv
WORKDIR /workspace
RUN chown ci:ci /workspace
COPY --chown=ci:ci . /workspace

USER ci
RUN /opt/venv/bin/python -m pip install -e ".[test]"

USER root
ENTRYPOINT ["/bin/bash", "/workspace/.github/workflows/ci-postgresql/run-tests.sh"]
