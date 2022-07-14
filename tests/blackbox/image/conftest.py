import sys
from threading import Thread

import requests
from pytest import fixture
from retrying import retry
from yellowbox import connect, image_build, temp_network

SERVICE_PORT = 80


@fixture
def base_url():
    return f'http://localhost:{SERVICE_PORT}/api'


@fixture
def v1_url(base_url):
    return base_url + '/v1'


@fixture
def container(docker_client, env_name, env_vars, redis, rabbitmq, blob_storage):
    image_build.build_image(docker_client, "biocatchtest/ayelet_initmanager", remove_image=True,
                            file=sys.stderr, spinner=True,
                            path=".")
    with temp_network(docker_client) as network, \
            connect(network, redis) as redis_alias, \
            connect(network, rabbitmq) as rabbit_alias, \
            connect(network, blob_storage):
        env_vars.redis_host = redis_alias[0]
        env_vars.rabbitmq_host = rabbit_alias[0]
        env_vars.connection_string = blob_storage.connection_string

        env = env_vars.as_dotenv()

        container = docker_client.containers.create(
            'biocatchtest/ayelet_initmanager:testing',
            ports={SERVICE_PORT: 0},
            environment=env)

        container.start()

        with connect(network, container):
            log_stream = container.logs(stream=True)

            def pipe():
                for line_b in log_stream:
                    line = str(line_b, 'utf-8').strip()
                    print(line, file=sys.stderr)

            pipe_thread = Thread(target=pipe)
            pipe_thread.start()

            @retry(stop_max_attempt_number=10, wait_fixed=3000)
            def check_health():
                response = requests.get(f'http://localhost:{SERVICE_PORT}/api/v1/health')
                try:
                    response.raise_for_status()
                except Exception:
                    print('init manager healthcheck failed, retrying...', response.content)
                    raise

            check_health()

            yield container
        container.kill('SIGKILL')
