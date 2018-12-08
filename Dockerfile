FROM python:3.7
ENV PYTHONUNBUFFERED 1
ARG REQUIREMENTS_FILE=requirements.txt

RUN mkdir /crawler-template
WORKDIR /crawler-template

ADD ${REQUIREMENTS_SRC_PATH} /crawler-template/
RUN pip install -e .
RUN pip install --no-cache-dir -r ${REQUIREMENTS_FILE}
ADD . /crawler-template/
