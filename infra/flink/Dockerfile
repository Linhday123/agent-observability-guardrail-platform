FROM flink:2.0.2-scala_2.12-java17

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir "apache-flink==2.0.0"
