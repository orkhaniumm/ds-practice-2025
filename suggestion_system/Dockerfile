# Use an official Python runtime as the base image
FROM python:3.11

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file to the working directory
COPY ./suggestion_system/requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the command to run the application using hotreload (for development)
CMD python utils/other/hotreload.py "suggestion_system/src/app.py"
