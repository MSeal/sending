# Sending 🧙🏽

> You send a short message of twenty-five words or less to a creature with which you are familiar.
> The creature hears the message in its mind, recognizes you as the sender if it knows you, and can answer in a like manner immediately.
> The spell enables creatures with Intelligence scores of at least 1 to understand the meaning of your message.
> - [dndbeyond.com](https://www.dndbeyond.com/spells/sending)

Sending is a package for supporting various [pub/sub implementations](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern).
It comes with some backends ready-made to be dropped into any async Python application, and provides the tools to support any backend
that isn't already accounted for.

It has three primary goals:
  1. Be highly concurrent. Sending should work in bite-sized chunks to ensure that applications that use it
     never feel blocked by its message processing.
  2. Be able to support a variety of transport mechanisms and pub/sub backends.
  3. Make it possible for applications to validate data coming in and out of their pub/sub implementations.

## Installation

By default, only the libraries required for the `InMemoryPubSubManager` backend are installed. For other backends, extra libraries are required. To support all backends, use the extras argument `all`.

```bash
# pip
pip install sending
# with a specific backend
pip install sending[jupyter]
# or all backends
pip install sending[all]

# poetry
poetry add sending
# with a specific backend
poetry add sending -E jupyter
# or all backends
poetry add sending -E all
```

## Developing

First run:

```bash
pip install nox
```

### Running Tests

```
nox -s test
```

To run tests against a specific backend you'll need to ensure that a local copy of that backend is running.

For redis, install the Redis server and then run `redis-server`. Afterwards you can run `nox -s test_redis` which will
run automated tests against your local instance of Redis. This assumes you are running using the default port of 6379.

### Linting

```
nox -s lint
```

### Style Enforcement

```
nox -s lint_check
```

## Websocket Logging

`WebsocketBackend` uses `structlog` for its logs. To view or suppress logs such as `Sending` and `Received` for all messages coming over the websocket, use this minimal structlog code.

```
import structlog
import logging

processors = [
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
]

structlog.configure(
    processors=processors, logger_factory=structlog.stdlib.LoggerFactory()
)

logging.basicConfig()
# enable debug logs
logging.getLogger("sending.backends.websocket").setLevel(logging.DEBUG)

# suppress debug logs
logging.getLogger("sending.backends.websocket").setLevel(logging.INFO)
```