FROM bitnami/spark:3.5.0

USER root

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create a non-root user with a home directory
RUN useradd -ms /bin/bash sparky && chown -R sparky:sparky /app

# Switch to the new user
USER sparky