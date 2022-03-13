FROM python:latest
LABEL Maintainer="paclflst"

WORKDIR /home

COPY ./ .
RUN pip install -r requirements.txt

CMD [ "python", "src/vet_import.py", "modelling-1.xlsx"]