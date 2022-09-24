import os
import consul
import pulsar
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

    client = pulsar.Client(f"pulsar://{get_config('PULSAR_SERVER')}")
    consumer = client.subscribe(get_config('PULSAR_TOPIC'), 'plex-library-updater',
                                consumer_type=pulsar.ConsumerType.Shared)

    while True:
        msg = consumer.receive()
        try:
            # decode from bytes, encode with backslashes removed, decode back to a string, then load it as a dict
            message_body = loads(msg.data().decode().encode('latin1', 'backslashreplace').decode('unicode-escape'))
            logger.info("Processing new message", extra={'message_body': message_body})
            process_message(message_body)
            logger.info("Done processing message", extra={'message_body': message_body})
            # Acknowledge successful processing of the message
            consumer.acknowledge(msg)
        except:  # noqa: E722
            # Message failed to be processed
            consumer.negative_acknowledge(msg)

    client.close()


def process_message(message_body):
    plex_server_host = get_config("PLEX_SERVER_HOST")
    plex_server_port = get_config("PLEX_SERVER_PORT")
    plex_token = get_config("PLEX_TOKEN")
    plex_url = 'http://{}:{}'.format(plex_server_host, plex_server_port)
    plex = PlexServer(plex_url, plex_token)
    if message_body['type'] == 'tv':
        library = plex.library.section('TV Shows')  # type: Library
    elif message_body['type'] == 'movie':
        library = plex.library.section('Movies')  # type: Library
    else:
        raise KeyError("Message type '{}' could not be mapped to a library".format(message_body['type']))
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
        'PULSAR_SERVER',
        'PULSAR_TOPIC',
        'PLEX_SERVER_HOST',
        'PLEX_SERVER_PORT',
        'PLEX_TOKEN',
    ]
    for config in required_configs:
        value = get_config(config)
        if not value:
            raise KeyError("Config key {} was not found in environment variables or Consul".format(config))


if __name__ == '__main__':
    check_configs()
    main()
