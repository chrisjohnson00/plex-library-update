# plex-library-update
A simple app that reads a Kafka topic and based on the message updates a Plex library

## PyPi Dependency updates

    pip install --upgrade pip
    pip install --upgrade pip kafka-python pygogo python-consul plexapi
    pip freeze > requirements.txt
    sed -i '/pkg-resources/d' requirements.txt
