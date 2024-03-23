from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import KafkaError
import logging
import json
import datetime
from redis_utils.redis_repository import RedisRepository
from utils.profiler import MyProfiler
import threading
import time


class GooveTempConsumer:

    def __init__(
        self,
        topic,
        consumer_config,
        redis_config,
        auto_report_interval=-1,
    ):

        default_config = {
            "auto_offset_reset": "earliest",
            "enable_auto_commit": True,
            "value_deserializer": lambda x: x.decode("utf-8"),
        }
        config = {**default_config, **consumer_config}
        self.topic = topic
        self.partition_index = 0
        self.partition_offset = -1
        self.consumer = KafkaConsumer(topic, **config)
        self.redis_repository = RedisRepository(redis_config)

        self.profiler = MyProfiler()
        self.is_profiler_setup = False
        self._setup_profiler()

        self.message_newest_ts = datetime.datetime.strptime(
            "1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"
        )

        self.enable_auto_report = True if auto_report_interval > 0 else False
        self.auto_report_lock = threading.Lock()
        if self.enable_auto_report:
            self.stop_event = threading.Event()
            self.status_report_thread = None
            self.auto_report_interval = auto_report_interval

    def start(self):
        try:

            logging.debug(f"Consumer Started.")
            self.profiler.start_time_metric("Total")
            self.profiler.start_precision_time_metric("Batch")
            self.profiler.start_precision_time_metric("idle_time")

            if self.enable_auto_report:
                self._start_async_reporting(self.auto_report_interval)

            # Process messages from the Kafka topic
            for message in self.consumer:
                try:


                    with self.auto_report_lock:
                        self.profiler.stop_precision_time_metric("idle_time")
                        self.partition_offset = message.offset

                    self.increment_counters("Total Messages", "Batch Messages")

                    # Extract the device ID and data from the message
                    device_id = message.key.decode("utf-8")
                    data = json.loads(message.value)

                    # Extract the timestamp
                    timestamp = data.get("timestamp")

                    # Update Latest Message ts
                    message_ts = datetime.datetime.strptime(
                        timestamp, "%Y-%m-%d %H:%M:%S"
                    )
                    if self.message_newest_ts < message_ts:
                        self.message_newest_ts = message_ts

                    # TODO: set expire to 48 hours
                    # Validate Dates
                    send_to_redis = False
                    date = datetime.datetime.strptime(
                        timestamp, "%Y-%m-%d %H:%M:%S"
                    ).date()
                    is_expired = self.redis_repository._is_expired(date, 2)

                    if not is_expired:

                        with self.auto_report_lock:
                            self.profiler.start_precision_time_metric(
                                "get_latest_timestamp"
                            )
                            latest_timestamp = (
                                self.redis_repository.get_latest_timestamp(device_id)
                            )
                            self.profiler.stop_precision_time_metric(
                                "get_latest_timestamp"
                            )

                        send_to_redis = (
                            not latest_timestamp
                            or timestamp > latest_timestamp.decode("utf-8")
                        )

                    if send_to_redis:

                        with self.auto_report_lock:
                            self.profiler.start_precision_time_metric("set_latest")
                            latest_setted = self.redis_repository.set_latest(
                                device_id, data
                            )
                            self.profiler.stop_precision_time_metric("set_latest")
                            if latest_setted:
                                self.increment_counters("Batch Latest", "Total Latest")

                        with self.auto_report_lock:
                            self.profiler.start_precision_time_metric("set_history")
                            history_setted = self.redis_repository.set_history(
                                device_id, data
                            )
                            self.profiler.stop_precision_time_metric("set_history")
                            if history_setted:
                                self.increment_counters(
                                    "Batch History", "Total History"
                                )

                        with self.auto_report_lock:
                            self.profiler.start_precision_time_metric("set_timeseries")
                            time_series_setted = self.redis_repository.set_timeseries(
                                device_id, data
                            )
                            self.profiler.stop_precision_time_metric("set_timeseries")
                            if time_series_setted:
                                self.increment_counters(
                                    "Batch TimeSeries", "Total TimeSeries"
                                )
                    else:
                        with self.auto_report_lock:
                            self.increment_counters(
                                "Batch Discarded", "Total Discarded"
                            )

                    with self.auto_report_lock:
                        self.profiler.start_precision_time_metric("idle_time")

                except Exception as e:
                    logging.error(e, exc_info=True)

        except Exception as e:
            logging.error(e, exc_info=True)

    def increment_counters(self, *counters):
        for c in counters:
            self.profiler.counter_increment(c)

    def report_status(self):

        with self.auto_report_lock:

            self.profiler.stop_precision_time_metric("Batch")
            idle_time_status = self.profiler.get_metric_status("idle_time")
            self.profiler.stop_precision_time_metric("idle_time")

            bm = self.profiler.get_counter_value("Batch Messages")
            bd = self.profiler.get_counter_value("Batch Discarded")
            bl = self.profiler.get_counter_value("Batch Latest")
            bh = self.profiler.get_counter_value("Batch History")
            bt = self.profiler.get_counter_value("Batch TimeSeries")

            tm = self.profiler.get_counter_value("Total Messages")
            td = self.profiler.get_counter_value("Total Discarded")
            tl = self.profiler.get_counter_value("Total Latest")
            th = self.profiler.get_counter_value("Total History")
            tt = self.profiler.get_counter_value("Total TimeSeries")

            etb = self.profiler.get_elapsed_time("Batch")
            etidle = self.profiler.get_total_elapsed_time("idle_time")
            etl = self.profiler.get_total_elapsed_time("set_latest")
            eth = self.profiler.get_total_elapsed_time("set_history")
            ett = self.profiler.get_total_elapsed_time("set_timeseries")
            etts = self.profiler.get_total_elapsed_time("get_latest_timestamp")
            trt = etl + eth + ett + etts  # total redis time

            pct = lambda x, total: (x / total) * 100 if total != 0 else 0
            thru = lambda total, elapsed: (total / elapsed) if elapsed != 0 else total

            if bm > 0:

                report_info = f" Report     : {bm} messages in the last {etb:.0f} seconds. Govee timestamp: '{self.message_newest_ts}' utc. Consuming offset {self.partition_offset}"
                logging.info("-" * len(report_info))
                logging.info(report_info)
                logging.info(
                    f" Msg Deltas : {bl} latest, {bh} history, {bt} timeSeries, {bd} discarded. "
                )
                logging.info(
                    f" Batch Time : {etidle:.1f}s idle time ({pct(etidle, etb):.1f}%), {trt:.2f}s redis time ({pct(trt, etb):.1f}%)."
                )
                logging.info(
                    f" Redis Time : {trt:.2f}s total, {etts:.1f}s getTimestamp ({pct(etts, trt):.1f}%), {etl:.1f}s setLatest ({pct(etl, trt):.1f}%), {eth:.1f}s setHistory ({pct(eth, trt):.1f}%), {ett:.1f}s setTimeSeries ({pct(ett, trt):.1f}%)."
                )
                logging.info(f" Redis Rate : {thru(bm,trt):.2f} messages/s.")
                logging.info(
                    f" Totals     : {tm} messages, {tl} latest, {th} history, {tt} timeSeries, {td} discarded."
                )
            else:
                logging.info(f"No new messages in the last {etb:.1f} seconds.")
                logging.info(f"Latest govee timestamp: '{self.message_newest_ts}' utc.")

            self.profiler.reset_counters(
                "Batch Messages",
                "Batch Discarded",
                "Batch Latest",
                "Batch History",
                "Batch TimeSeries",
            )
            self.profiler.reset_metrics(
                "Batch",
                "get_latest_timestamp",
                "set_latest",
                "set_history",
                "set_timeseries",
                "idle_time",
            )

            if idle_time_status == "running":
                self.profiler.start_precision_time_metric("idle_time")

            self.profiler.start_precision_time_metric("Batch")

    def report_periodically(self, interval):
        if not self.enable_auto_report:
            return
        while True:
            time.sleep(interval)
            self.report_status()

    def _start_async_reporting(self, interval):
        if not self.enable_auto_report:
            return

        if self.status_report_thread is None:
            self.stop_event.clear()
            self.status_report_thread = threading.Thread(
                target=self.report_periodically, args=(interval,), daemon=True
            )
            self.status_report_thread.start()
            logging.info(f"Reporting status every {interval} seconds.")

    def _stop_async_reporting(self):
        if not self.enable_auto_report:
            return
        self.status_report_thread = None
        logging.info("Asynchronous reporting stopped.")

    def close(self):
        try:
            self.consumer.close()
            logging.debug("Consumer closed")
        except KafkaError as e:
            logging.error(f"Error closing consumer: {e}")

        try:
            self._stop_async_reporting()
            # logging.debug("_stop_async_reporting")
        except Exception as e:
            logging.error(f"Error stoting autoreporting: {e}")

    def _setup_profiler(self):
        if self.is_profiler_setup:
            return
        self.is_profiler_setup = True
        self.profiler.add_counters(
            "Batch Messages",
            "Batch Discarded",
            "Batch Latest",
            "Batch History",
            "Batch TimeSeries",
        )
        self.profiler.add_counters(
            "Total Messages",
            "Total Discarded",
            "Total Latest",
            "Total History",
            "Total TimeSeries",
        )
        self.profiler.add_time_metrics("Total", "Batch")
        self.profiler.add_time_metrics(
            "set_latest",
            "set_history",
            "set_timeseries",
            "get_latest_timestamp",
            "idle_time",
        )

    def log_latest_offset(self):
        topic_partition = TopicPartition(self.topic, self.partition_index)
        committed_offset = self.consumer.committed(topic_partition)
        logging.info(f"Last committed offset for {topic_partition}: {committed_offset}")
