# Down this path madness lies:
# https://uwekorn.com/2021/03/01/deploying-conda-environments-in-docker-how-to-do-it-right.html

FROM mambaorg/micromamba:1.3.1
COPY --chown=$MAMBA_USER:$MAMBA_USER env.yaml /tmp/env.yaml
RUN --mount=type=cache,target=/opt/conda/pkgs micromamba install -y -n base -f /tmp/env.yaml && \
#    micromamba remove -n base --force scipy boto3 botocore && \
    micromamba run -n base python -m pip install --no-cache --no-deps datacube-explorer flask-themer && \
    micromamba clean --all --yes --force-pkgs-dirs && \
    find /opt/conda/lib/python3.11/site-packages/scipy -name 'tests' -type d -exec rm -rf '{}' '+' && \
    find /opt/conda/lib/python3.11/site-packages/numpy -name 'tests' -type d -exec rm -rf '{}' '+' && \
    find /opt/conda/lib/python3.11/site-packages/pandas -name 'tests' -type d -exec rm -rf '{}' '+' && \
    find /opt/conda/lib/python3.11/site-packages/xarray -name 'tests' -type d -exec rm -rf '{}' '+' && \
    find /opt/conda/lib/python3.11/site-packages/dask -name 'tests' -type d -exec rm -rf '{}' '+' && \
    find /opt/conda/lib/python3.11/site-packages/tornado -name 'test' -type d -exec rm -rf '{}' '+' && \
    find /opt/conda/lib/python3.11/site-packages -name '*.pyx' -delete && \
    rm -rf \
       /opt/conda/{include,man,conda-meta} \
       /opt/conda/share/{poppler,doc,man,terminfo,X11} \
       /opt/conda/lib/libpython3.11.so.1.0 \
       /opt/conda/bin/pdf* /opt/conda/bin/postgres \
       /opt/conda/lib/python3.11/idlelib \
       /opt/conda/lib/python3.11/site-packages/botocore/data && \
    find /opt/conda/ -name '__pycache__' -type d -exec rm -rf '{}' '+' && \
    rm -rf /opt/conda/lib/python3.11/site-packages/pip /opt/conda/lib/python3.11/ensurepip
#ARG MAMBA_DOCKERFILE_ACTIVATE=1  # (otherwise python will not be found)

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
