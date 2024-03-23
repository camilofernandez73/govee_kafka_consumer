import logging
import time
from utils.config_manager import load_config
from kafka.errors import KafkaError
from consumers.goove_temp_consumer import GooveTempConsumer


def main():

    try:
        # Load the configuration
        config = load_config()

        # Kafka producer configuration
        topic_name = config["topic_name"]
        logging.info(f"Consuming topic: '{topic_name}'.")

        redis_config = config["redis_config"]

        consumer_config = {
            "bootstrap_servers": config["bootstrap_servers"],
            "group_id": "govee-temp-consumer-group",
        }

        consumer = GooveTempConsumer(
            topic_name,
            consumer_config,
            redis_config,
            auto_report_interval=config["report_interval"],
        )
        
        consumer.start()
 


    except KeyboardInterrupt:
        logging.info("Interrupted by user. Stopping consumer...")

    except KafkaError as e:
        logging.error(e, exc_info=True)

    except Exception as e:
        logging.error(e, exc_info=True)

    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
