# The Airflow image, extended with what the Lens pipeline actually needs.
#
# Built rather than using _PIP_ADDITIONAL_REQUIREMENTS, which upstream's own
# compose file warns is for quick checks only: it reinstalls on every container
# start, so a scheduler restart silently becomes a dependency resolution and a
# transient PyPI failure becomes a broken stack.
#
# Java is here because the normalise step is PySpark, and Spark needs a JVM even
# in local mode. It costs roughly 200 MB and is only exercised when the landing
# zone has actually changed - see the DAG's short-circuit.
#
# The package itself is NOT installed. The repository is bind-mounted and
# PYTHONPATH points at its src directory, so the code that runs is the code
# under version control. Installing it at build time would bake a copy that
# drifts from the working tree the moment either is edited.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

FROM apache/airflow:3.3.0

USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y openjdk-17-jre-headless procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/codrona-requirements.txt
RUN pip install --no-cache-dir -r /tmp/codrona-requirements.txt
