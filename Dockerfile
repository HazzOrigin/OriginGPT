# Start from a clean, small Python image (3.11 is a good, modern choice)
FROM python:3.11-slim

# Set the working directory inside the container to '/app'
WORKDIR /app

# Copy the file that lists our tools and install them
# This is a key step that saves time by installing dependencies first
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the main instruction file into the container
COPY main.py .

# Command to run the script when the container starts (the Cloud Run Job command)
# It simply tells Python to run your main script once.
CMD ["python", "main.py"]
