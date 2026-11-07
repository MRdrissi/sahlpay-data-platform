# Image Spark identique à celle du cluster
FROM ghcr.io/apache/spark-docker/spark:3.5.7-scala2.12-java17-python3-ubuntu

# Installer les outils Python nécessaires
USER root

RUN python3 -m pip install --no-cache-dir \
    "jupyterlab==4.4.10" \
    "ipykernel>=6.29,<7" \
    "py4j==0.10.9.7"


# US-03 : API Python de Delta Lake et de GraphFrames
# --no-deps : PySpark est déjà fourni par l'image Spark (/opt/spark/python)
# numpy : requis par pyspark.ml, utilisé par GraphFrames
RUN python3 -m pip install --no-cache-dir --no-deps \
    "delta-spark==3.3.2" \
    "graphframes-py==0.9.2" \
    "numpy==1.26.4" \
    "importlib_metadata==8.5.0" \
    "zipp==3.21.0"

# Préparer le répertoire de travail
RUN mkdir -p /home/spark/notebooks /home/spark/.ivy2 && \
    chown -R spark:spark /home/spark

# Configuration de Python et Spark
ENV HOME=/home/spark \
    PYTHONPATH=/opt/spark/python \
    PYSPARK_PYTHON=python3

# Exécution avec un utilisateur non privilégié
USER spark

WORKDIR /home/spark/notebooks

# Démarrer JupyterLab
CMD ["python3", "-m", "jupyterlab", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser"]