# Stage 1: builder — cài dependency
FROM python:3.11-slim AS builder

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --user -r requirements.txt


# Stage 2: runtime — chỉ copy dependency đã cài
FROM python:3.11-slim AS runtime

WORKDIR /app

COPY --from=builder /root/.local /root/.local
COPY main.py .

ENV PATH=/root/.local/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
