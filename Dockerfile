FROM python:3.6-alpine

ENV INSTALL_PATH /persona_db_bootstrap
RUN mkdir -p ${INSTALL_PATH}
WORKDIR ${INSTALL_PATH}

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

CMD python db_bootstrap.py