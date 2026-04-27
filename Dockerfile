FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1 \
	PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt .
RUN python -m pip install --upgrade pip \
	&& python -m pip install -r requirements.txt

# Copy bot source.
COPY . .

# Run as non-root and ensure runtime directories are writable.
RUN addgroup --system openpotd \
	&& adduser --system --ingroup openpotd openpotd \
	&& mkdir -p /app/data /app/config \
	&& chown -R openpotd:openpotd /app

USER openpotd

VOLUME ["/app/data", "/app/config"]

CMD ["python", "-u", "openpotd.py"]
