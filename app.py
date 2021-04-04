import os
import consul
from kafka import KafkaConsumer
from json import loads
import os.path
import pygogo as gogo
from plexapi.server import PlexServer
from plexapi.library import Library

CONFIG_PATH = "plex-updater"
# logging setup
kwargs = {}
formatter = gogo.formatters.structured_formatter
logger = gogo.Gogo('struct', low_formatter=formatter).get_logger(**kwargs)


def main():
    logger.info("Starting!!")

    consumer = KafkaConsumer(
        get_config("KAFKA_TOPIC"),
        bootstrap_servers=[get_config('KAFKA_SERVER')],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=get_config("KAFKA_CONSUMER_GROUP"),
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    for message in consumer:
        message_body = message.value
        logger.info("Processing new message", extra={'message_body': message_body})
        process_message(message_body)
        logger.info("Done processing message", extra={'message_body': message_body})
        # force commit
        consumer.commit_async()


def process_message(message_body):
    plex_server_host = get_config("PLEX_SERVER_HOST")
    plex_server_port = get_config("PLEX_SERVER_PORT")
    plex_token = get_config("PLEX_TOKEN")
    plex_url = 'http://{}:{}'.format(plex_server_host, plex_server_port)
    plex = PlexServer(plex_url, plex_token)
    if message_body['type'] == 'tv':
        library = plex.library.section('TV Shows')  # type: Library
    elif message_body['type'] == 'movies':
        library = plex.library.section('Movies')  # type: Library
    logger.info("Library update", extra={'library': library.title})
    library.update()


def get_config(key, config_path=CONFIG_PATH):
    if os.environ.get(key):
        logger.info("found {} in an environment variable".format(key))
        return os.environ.get(key)
    logger.info("looking for {}/{} in consul".format(config_path, key))
    c = consul.Consul()
    index, data = c.kv.get("{}/{}".format(config_path, key))
    return data['Value'].decode("utf-8")


def check_configs():
    required_configs = [
        'KAFKA_TOPIC',
        'KAFKA_SERVER',
        'KAFKA_CONSUMER_GROUP',
        'PLEX_SERVER_HOST',
        'PLEX_SERVER_PORT',
        'PLEX_TOKEN',
    ]
    for config in required_configs:
        value = get_config(config)
        if not value:
            raise KeyError("Config key {} was not found in environment variables or Consul")


if __name__ == '__main__':
    check_configs()
    main()
