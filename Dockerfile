FROM python:3.10-slim

RUN apt-get update && apt-get install -y build-essential default-libmysqlclient-dev libpq-dev wget unzip

WORKDIR /workspace
COPY requirements.txt /workspace/

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

CMD ["bash"]
