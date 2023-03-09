FROM mambaorg/micromamba:1.3.1
COPY --chown=$MAMBA_USER:$MAMBA_USER env.yaml /tmp/env.yaml
RUN micromamba install -y -n base -f /tmp/env.yaml && \
#    micromamba remove -n base --force scipy boto3 botocore && \
    micromamba clean --all --yes --force-pkgs-dirs && \
    rm -rf \
       /opt/conda/{include,man,conda-meta} \
       /opt/conda/share/{poppler,doc,man,terminfo,X11} \
       /opt/conda/bin/pdf* /opt/conda/bin/postgres \
       /opt/conda/lib/python3.11/site-packages/botocore/data \
       /opt/conda/lib/python3.11/site-packages/pandas/tests
ARG MAMBA_DOCKERFILE_ACTIVATE=1  # (otherwise python will not be found)
RUN pip install --no-cache --no-deps datacube-explorer flask-themer

#CMD ["micromamba", "run", "-n", "base", "gunicorn", "-k", "uvicorn.workers.UvicornWorker"]
# #"micromamba", "run", "-n", "base", \
CMD ["gunicorn", \
     "-b", \
     "0.0.0.0:8080", \
     "-w", \
     "3", \
     "--threads=2", \
     "-k", \
     "gthread", \
     "--timeout", \
     "90", \
     "--config", \
     "python:cubedash.gunicorn_config", \
     "cubedash:app"]
