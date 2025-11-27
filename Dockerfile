FROM apache/airflow:latest

USER root

RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y make gcc && \
    apt-get -y install p7zip-full poppler-utils qpdf && \
    apt-get update && \
    apt-get upgrade -y && \
    apt-get clean && rm -rf /var/cache/apt/* /var/lib/apt/lists/*

COPY requirements.txt /opt/airflow

RUN chown -R airflow /opt/airflow
RUN chown -R airflow /home/airflow

USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /opt/airflow/requirements.txt

RUN mkdir /opt/airflow/Tools

WORKDIR /opt/airflow/Tools

RUN curl https://didierstevens.com/files/software/pdf-parser_V0_7_13.zip -o pdf-parser.zip && 7z e pdf-parser.zip && chmod +x pdf-parser.py
RUN curl https://didierstevens.com/files/software/pdfid_v0_2_10.zip -o pdfid.zip && 7z e pdfid.zip && chmod +x pdfid.py
RUN curl https://didierstevens.com/files/software/pdftool_V0_0_1.zip -o pdftool.zip && 7z e pdftool.zip && chmod +x pdftool.py
RUN touch __init__.py

RUN rm -rf /home/airflow/.local/lib/python3.12/site-packages/airflow/example_dags/example*
RUN rm -rf /home/airflow/.local/lib/python3.12/site-packages/airflow/example_dags/tutorial*
RUN rm -rf /home/airflow/.local/lib/python3.12/site-packages/airflow/example_dags/standard/example*

WORKDIR /home/airflow/work

ENV TOOLS_PATH=/opt/airflow/Tools
ENV WORKDIR_WORKFLOW=/home/airflow/work
ENV PYTHONPATH=/opt/airflow/Tools

