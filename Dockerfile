# Use Playwright’s official image (includes Chromium + fonts)
FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

# Set workdir
WORKDIR /app

# Copy files
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app
COPY . /app

# Expose port
EXPOSE 8000

# Default command
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
