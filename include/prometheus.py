from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway
from prometheus_client.exposition import basic_auth_handler


def send_metric(
    PROMETHEUS_CONFIG: dict,
    base_labels: dict,
    metric_name: str,
    metric_type: str,
    metric_value,
    metric_description: str = "",
    additional_labels: dict = {},
) -> None:

    if not PROMETHEUS_CONFIG["endpoint"]:
        return

    FULL_METRIC_NAME = f"nodestatus_{metric_name}"
    labels = {**base_labels, **additional_labels}
    registry = CollectorRegistry()

    if metric_type == "gauge":
        gauge = Gauge(
            FULL_METRIC_NAME,
            metric_description,
            labelnames=labels.keys(),
            registry=registry,
        )
        gauge.labels(**labels).set(metric_value)

    else:
        print(f"Unknown metric {metric_type}")
        return

    job = f'nodestatus-{metric_name}-{base_labels["producer"]}'
    job = "nodestatus"
    pushadd_to_gateway(PROMETHEUS_CONFIG["endpoint"], job=job, registry=registry)


def send_bpjson_accessible_metric(
    PROMETHEUS_CONFIG, common_prometheus_labels, metric_value
):
    send_metric(
        PROMETHEUS_CONFIG,
        common_prometheus_labels,
        "bpjson_accessible",
        "gauge",
        metric_value,
        "BP json accessible",
    )


def send_bpjson_status_metric(
    PROMETHEUS_CONFIG, common_prometheus_labels, metric_value
):
    send_metric(
        PROMETHEUS_CONFIG,
        common_prometheus_labels,
        "bpjson_status",
        "gauge",
        metric_value,
        "BP json response status",
    )


def send_bpjson_response_time_metric(
    PROMETHEUS_CONFIG, common_prometheus_labels, metric_value
):
    send_metric(
        PROMETHEUS_CONFIG,
        common_prometheus_labels,
        "bpjson_response_time",
        "gauge",
        metric_value,
        "BP json response time",
    )
