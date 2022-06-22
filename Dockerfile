FROM python:3.8
WORKDIR /code
COPY ./app /code/app
COPY ./utils /code/utils
COPY pyproject.toml /code
ENV PYTHONPATH=${PYTHONPATH}:${PWD}
RUN pip3 install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]

