import logging

from _pytest.fixtures import fixture
from docker import DockerClient
from pydantic.dataclasses import dataclass
from yellowbox import temp_network
from yellowbox.extras.azure_storage import BlobStorageService
from yellowbox.extras.rabbit_mq import RabbitMQService
from yellowbox.extras.redis import RedisService

logger = logging.getLogger('biocatch.' + __name__)


@dataclass
class BlackboxEnv:
    env_name: str = ""
    redis_host: str = ""
    redis_port: str = "6379"
    rabbitmq_host: str = ""
    rabbitmq_port: str = "5672"
    rabbitmq_exchange: str = "initmanagerblackbox"
    rabbitmq_queue_name: str = ""
    rabbitmq_queue_username: str = ""
    rabbitmq_queue_password: str = ""
    connection_string: str = ""
    container_name: str = ""
    profiles_prefix: str = ""
    ds_profiles_prefix: str = ""

    def as_dotenv(self):
        env = [
            f'EnvironmentName={self.env_name}',

            f'RedisConnectionString={self.redis_host}',
            'RedisPassword=',
            f'redis_port={self.redis_port}',
            f'redis_host={self.redis_host}',

            f'rabbitmq_port={self.rabbitmq_port}',
            f'rabbitmq_host={self.rabbitmq_host}',
            f'rabbitmq_exchange={self.rabbitmq_exchange}',
            f'rabbitmq_queue_name={self.rabbitmq_queue_name}',
            f'RABBIT_URL=amqp://guest:guest@{self.rabbitmq_host}:{self.rabbitmq_port}/guest',
            'READ_FROM_RABBIT=true',


            f'connection_string={self.connection_string}',

            f'container_name={self.container_name}',
            f'profiles_prefix={self.profiles_prefix}',
            f'ds_profiles_prefix={self.ds_profiles_prefix}',
            "PYTHONUNBUFFERED=1"
        ]

        return env


@fixture
def env_name() -> str:
    return 'testenv'


@fixture
def env_vars(env_name: str) -> BlackboxEnv:
    return BlackboxEnv(env_name=env_name)


@fixture
def redis(docker_client: DockerClient) -> RedisService:
    with RedisService.run(docker_client) as service:
        yield service


@fixture
def network(docker_client):
    with temp_network(docker_client) as network:
        yield network


@fixture
def rabbitmq(docker_client, network) -> RabbitMQService:
    with RabbitMQService.run(docker_client, enable_management=True) as service:
        yield service


@fixture(scope="session")
def blob_storage(docker_client):
    with BlobStorageService.run(docker_client, image="mcr.microsoft.com/azure-storage/azurite:3.17.1") as service:
        yield service
