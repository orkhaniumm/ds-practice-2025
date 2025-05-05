FROM python:3.11

WORKDIR /app

# Copy the requirements file and install dependencies.
COPY ./payment/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy PaymentService source code.
COPY ./payment/src/ /app/payment/src/

# Copy the PaymentService proto stubs
COPY ./utils/pb/payment/payment_pb2.py /app/payment_pb2.py
COPY ./utils/pb/payment/payment_pb2_grpc.py /app/payment_pb2_grpc.py

# Run the PaymentService
CMD ["python", "payment/src/app.py"]
