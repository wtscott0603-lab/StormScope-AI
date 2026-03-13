FROM mambaorg/micromamba:1.5.8

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY --chown=$MAMBA_USER:$MAMBA_USER backend/processor/environment.yml /tmp/environment.yml
RUN micromamba install -y -n base -f /tmp/environment.yml && \
    micromamba clean --all --yes

COPY --chown=$MAMBA_USER:$MAMBA_USER backend /app/backend

CMD ["python", "-m", "backend.processor.main"]
