FROM python:3.11.3-bullseye

COPY requirements.txt /app/

WORKDIR /app

RUN pip install -r requirements.txt

CMD [ "airflow", "server", "-p", "8080" ]