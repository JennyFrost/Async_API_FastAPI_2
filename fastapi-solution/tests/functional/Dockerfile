FROM python:3.10

WORKDIR /usr/src/test

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt

RUN apt-get update && \
    apt-get clean

RUN  pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

RUN chmod +x utils/run_tests.sh

CMD ["utils/run_tests.sh"]
