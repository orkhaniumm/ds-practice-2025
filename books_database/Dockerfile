FROM python:3.11

WORKDIR /app

COPY ./books_database/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY ./utils /app/utils
COPY ./books_database/src /app/books_database/src

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["python", "books_database/src/app.py"]