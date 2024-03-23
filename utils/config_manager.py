import json
import os
import logging


def load_config():
    config_path = "config.json"

    try:
        with open(config_path) as config_file:
            config = json.load(config_file)

        default_logging_level = config.get("logging_level", "INFO")
        env_key = config.get("ENV_KEY")
        env = os.getenv(env_key, "development")
        env_config = config.get(env)

        if not env_config:
            raise ValueError(f"Configuration for environment {env} not found.")

        # Set the log output format
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        logging_level = env_config.get("logging_level", default_logging_level)
        logging.getLogger().setLevel(logging.getLevelName(logging_level))
        logging.debug(f"Environment set to '{env}'.")

        return env_config

    except FileNotFoundError:
        logging.critical(f"Configuration file not found: {config_path}")
        raise

    except json.JSONDecodeError:
        logging.critical(f"Invalid JSON format in configuration file: {config_path}")
        raise

    except ValueError as e:
        logging.critical(str(e))
        raise

    except Exception as e:
        logging.critical(f"Unexpected error: {str(e)}")
        raise
