FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential gcc gfortran && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt
COPY . /app
CMD ["python", "run_sim.py", "--loop"]
