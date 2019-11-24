# Using techniques here: https://jcrist.github.io/conda-docker-tips.html to reduce image size

FROM continuumio/miniconda3

ENV PYTHONDONTWRITEBYTECODE=true

RUN conda install --yes \
    nomkl \
    numpy=1.17.2 \
    boto3=1.9.234 \
    pandas=0.25.1 \
    && conda clean -afy \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete

COPY fpltools /fpltools/
COPY scripts/run_data_transform.py ./

CMD ["python", "run_data_transform.py"]
