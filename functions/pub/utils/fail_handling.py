
import logging

def fail(message: str):
    logging.error(message)
    raise ValueError(message)   # Durable-safe deterministic fail
