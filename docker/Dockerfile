FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY requirements-docker.txt .
RUN pip install --no-cache-dir -r requirements-docker.txt

COPY src/ ./src/
COPY data/ ./data/

RUN useradd -m -u 1000 dataeng && \
    chown -R dataeng:dataeng /app
USER dataeng

CMD ["python", "src/etl.py"]
