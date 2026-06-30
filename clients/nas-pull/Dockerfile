FROM python:3-slim

# Dependency-free client (Python stdlib only): no pip install step needed.
WORKDIR /app
COPY stoarama_pull.py /app/stoarama_pull.py

# Unbuffered stdout so logs stream live to Container Manager.
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python3", "/app/stoarama_pull.py"]
