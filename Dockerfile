FROM python:3.12
ENV PYTHONUNBUFFERED TRUE
WORKDIR /app
COPY ./requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade -r requirements.txt
COPY . /app
CMD gunicorn -k uvicorn.workers.UvicornWorker main:app -b 0.0.0.0:7860 --timeout 300 --threads 2 --workers 2
