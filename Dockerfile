# Dockerfile
# Playwright base image (includes Chromium, dependencies & fonts)
FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

WORKDIR /app

# Install Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app
COPY . /app

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
