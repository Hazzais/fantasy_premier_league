# Using techniques here: https://jcrist.github.io/conda-docker-tips.html to reduce image size

FROM continuumio/miniconda3

ENV PYTHONDONTWRITEBYTECODE=true

# Small footprint to stay within AWS free tier for ECR! Ideally just install using environment.yml.
RUN conda install --yes \
    nomkl \
    numpy=1.17.2 \
    boto3=1.9.234 \
    pandas=0.25.1 \
    && conda clean -afy \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete

# Requires building Dockerfile from parent directory
# docker build -t <some tag> -f <dir/dir/Dockerfile> . 
COPY fpltools /fpltools/
COPY etl ./

CMD ["python", "run_data_transform_aws.py"]
