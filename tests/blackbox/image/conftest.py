import json
import sys
from threading import Thread

from pytest import fixture
from yellowbox import connect, temp_network

SERVICE_PORT = 80


@fixture
def base_url():
    return f'http://localhost:{SERVICE_PORT}/api'


@fixture
def v1_url(base_url):
    return base_url + '/v1'


@fixture
def im(docker_client, env_name, env_vars, redis, rabbitmq, blob_storage):

    build_log = docker_client.api.build(path=".",
                                        tag='biocatchtest/ayelet_initmanager:testing',
                                        rm=True)
    for msg_b in build_log:
        msgs = str(msg_b, 'utf-8').splitlines()
        for msg in msgs:
            s = json.loads(msg).get('stream')
            if s:
                print(s, end='', flush=True, file=sys.stderr)
            else:
                print(msg, file=sys.stderr)

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
            ports={SERVICE_PORT: SERVICE_PORT},
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

            yield container
        container.kill('SIGKILL')
