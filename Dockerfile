# *******PYTHON BASE IMAGE*******
FROM python:3.9.17-slim-bullseye as python
 
# Run installation tasks as root
USER root

# Project folder name
ARG PROJECT_FOLDER=/opt/ufo-lakehouse

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Specify the user info for spark_uid
RUN useradd -d /home/sparkuser -ms /bin/bash sparkuser

# Install Java/procps
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=${JAVA_HOME}/bin:${PATH}

# Set SPARK_HOME
ENV SPARK_HOME=/usr/local/lib/python3.9/site-packages/pyspark
ENV PATH=${SPARK_HOME}/bin:${PATH}

# Install pip requirements
COPY ./requirements.txt ${PROJECT_FOLDER}/requirements.txt
RUN python -m pip install --no-cache-dir -r ${PROJECT_FOLDER}/requirements.txt

# Copy local directory into container app directory
WORKDIR ${PROJECT_FOLDER}
COPY . ${PROJECT_FOLDER}

# Set appropriate ownership for the user within the container
RUN chown -R sparkuser:sparkuser ${PROJECT_FOLDER}

# switch to sparkuser
USER sparkuser

# Start JupyterLab at container start
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser"]


# *******SPARK BASE IMAGE*******
FROM bitnami/spark:3.4.0 as spark

# Project folder name
ARG PROJECT_FOLDER=/opt/ufo-lakehouse

USER root

# Install pip requirements
COPY ./requirements.txt ${PROJECT_FOLDER}/requirements.txt
RUN pip install --no-cache-dir --target /opt/bitnami/python/lib/python3.9/site-packages -r ${PROJECT_FOLDER}/requirements.txt