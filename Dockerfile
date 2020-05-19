FROM python:3.7.1

RUN pip3 install prometheus_client
RUN pip3 install pyaml

COPY exporter/ /opt/exporter/
RUN chmod 755 /opt/exporter/exporter.py

RUN useradd -m -s /bin/bash my_user

USER my_user

ENTRYPOINT ["/usr/local/bin/python", "/opt/exporter/exporter.py"]
