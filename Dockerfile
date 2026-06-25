# Fleet Commander — multi-stage build.

# ---- builder ----
FROM python:3.12-slim AS builder
WORKDIR /app
# pyproject uses a src-layout (packages.find where=["src"]) and references README.md
# for its long description, so both must be present before the install resolves.
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# ---- runtime ----
FROM python:3.12-slim
WORKDIR /app
RUN useradd -r -u 10001 fc
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY src/ ./src/
COPY config/config.example.yaml /app/config/config.yaml
# Create the state dir owned by fc so the fc_state named volume initializes with
# fc ownership (Docker seeds a fresh named volume from the image path); otherwise
# the non-root fc user cannot create the SQLite file under a root-owned mount.
RUN mkdir -p /app/state && chown -R fc:fc /app/state
ENV PYTHONUNBUFFERED=1
EXPOSE 8080
# Note: running as non-root requires the fc user to be in the host docker group
# (or use a socket proxy). See the "Docker socket is root-equivalent" note in README.md.
USER fc
CMD ["python", "-m", "fc.main"]
