FROM apache/spark:3.5.6

USER root

RUN python3 -m pip install --no-cache-dir pytest

ENV PYTHONPATH=/workspace:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /workspace

USER spark
