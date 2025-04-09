# Use an official Python runtime as the base image
FROM python:3.11

WORKDIR /app

# Copy the requirements file and install dependencies
COPY ./order_queue/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application source code
COPY ./order_queue/src/ /app/order_queue/src/

# Copy generated proto stubs from utils into the image
COPY ./utils/pb/order_queue/order_queue_pb2.py /app/order_queue_pb2.py
COPY ./utils/pb/order_queue/order_queue_pb2_grpc.py /app/order_queue_pb2_grpc.py

# Run the Order Queue application directly
CMD ["python", "order_queue/src/app.py"]
