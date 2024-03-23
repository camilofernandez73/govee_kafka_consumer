import time
import logging

class MyProfiler:

    def __init__(self):
        self.time_metrics = {}
        self.counters = {}
        self.heartbeat_enable = False
        self.logger = logging.getLogger("Profiler")


    def add_counters(self, *counter_names):
        for name in counter_names:
            self.counters[name] = 0

    def add_time_metrics(self, *metric_names):
        for name in metric_names:
           self.add_time_metric(name)

    def add_time_metric(self, metric_name):
        self.time_metrics[metric_name] = {
            "start_time": 0,
            "end_time": 0,
            "elapsed_time": 0,
            "total_elapsed_time": 0,
            "status": 'stopped',
        }
        return self.time_metrics[metric_name]

    def start_time_metric(self, metric_name):
        metric = self.time_metrics.get(metric_name)
        metric["start_time"] = time.monotonic()
        metric["status"] = 'running'
 

    def stop_time_metric(self, metric_name):
        end_time = time.monotonic()
        metric = self.time_metrics[metric_name]
        if metric["status"] == 'running':
            metric["end_time"] = end_time
            metric["elapsed_time"] = metric["end_time"] - metric["start_time"]
            metric["total_elapsed_time"] += metric["elapsed_time"]
            metric["status"] = 'stopped'

    def stop_time_metrics(self, *metric_names):
        end_time = time.monotonic()
        for name in metric_names:
            metric = self.time_metrics.get(name)
            if metric["status"] == 'running':
                metric["end_time"] = end_time
                metric["elapsed_time"] = metric["end_time"] - metric["start_time"]
                metric["total_elapsed_time"] += metric["elapsed_time"]
                metric["status"] = 'stopped'
            else:
                self.logger.debug(f"Attempting to stop an already stopped metric.Metric: {name}.")
            

    def start_precision_time_metric(self, metric_name):
        start_time = time.perf_counter()
        metric = self.time_metrics.get(metric_name)
        metric["start_time"] = start_time
        metric["status"] = 'running'

    def stop_precision_time_metric(self, metric_name):
        end_time = time.perf_counter()
        metric = self.time_metrics.get(metric_name)
        if metric["status"] == 'running':
            metric["end_time"] = end_time
            metric["elapsed_time"] = metric["end_time"] - metric["start_time"]
            metric["total_elapsed_time"] += metric["elapsed_time"]
            metric["status"] = 'stopped'
        else:
            self.logger.debug(f"Attempting to stop an already stopped metric.Metric: {metric_name}.")

    def get_elapsed_time(self, metric_name):
        metric = self.time_metrics.get(metric_name)
        return metric["elapsed_time"]

    def get_total_elapsed_time(self, metric_name):
        metric = self.time_metrics.get(metric_name)
        return metric["total_elapsed_time"]

    def get_metric_status(self, metric_name):
        metric = self.time_metrics.get(metric_name)
        return metric["status"]
    
    def get_counter_value(self, counter_name):
        return self.counters[counter_name]

    def reset_metrics(self, *metric_names):
        for name in metric_names:
            metric = self.time_metrics.get(name)
            metric["start_time"] = 0
            metric["end_time"] = 0
            metric["elapsed_time"] = 0
            metric["total_elapsed_time"] = 0
            metric["status"] = 'stopped'

    def reset_counters(self, *counters):
        for cname in counters:
            self.counters[cname] = 0

    def counter_increment(self, name):
        self.counters[name] += 1

    def counters_increment(self, *counters):
        for cname in counters:
            self.counter_increment(cname)

