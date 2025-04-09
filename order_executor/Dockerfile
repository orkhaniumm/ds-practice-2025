FROM python:3.11

WORKDIR /app

# Copy the requirements file and install dependencies.
COPY ./order_executor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Order Executor source code.
COPY ./order_executor/src/ /app/order_executor/src/

# Copy the Order Executor proto stubs
COPY ./utils/pb/order_executor/order_executor_pb2.py /app/order_executor_pb2.py
COPY ./utils/pb/order_executor/order_executor_pb2_grpc.py /app/order_executor_pb2_grpc.py

COPY ./utils/pb/order_queue/order_queue_pb2.py /app/order_queue_pb2.py
COPY ./utils/pb/order_queue/order_queue_pb2_grpc.py /app/order_queue_pb2_grpc.py

# Set the command to run the Order Executor application directly.
CMD ["python", "order_executor/src/app.py"]
