# Sending 🧙🏽

> You send a short message of twenty-five words or less to a creature with which you are familiar.
> The creature hears the message in its mind, recognizes you as the sender if it knows you, and can answer in a like manner immediately.
> The spell enables creatures with Intelligence scores of at least 1 to understand the meaning of your message.
- [dndbeyond.com](https://www.dndbeyond.com/spells/sending)

Sending is a package for supporting various [pub/sub implementations](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern).
It comes with some backends ready-made to be dropped into any async Python application, and provides the tools to support any backend
that isn't already accounted for.

It has three primary goals:
  1. Be highly concurrent. Sending should work in bite-sized chunks to ensure that applications that use it
     never feel blocked by its message processing.
  2. Be able to support a variety of transport mechanisms and pub/sub backends.
  3. Make it possible for applications to validate data coming in and out of their pub/sub implementations.

## Installation
TODO

## Example Usage
TODO
