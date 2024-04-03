import typing as t

from opentelemetry import metrics as metrics_api
from opentelemetry.metrics import _internal as internal_api
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics._internal.point import Metric
from opentelemetry.sdk.metrics.export import DataPointT
from opentelemetry.sdk.metrics.export import HistogramDataPoint
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.metrics.export import NumberDataPoint
from opentelemetry.util import types as otel_types
from opentelemetry.util._once import Once


# Adapted from https://github.com/open-telemetry/opentelemetry-python/blob/main/tests/opentelemetry-test-utils/src/opentelemetry/test/test_base.py  # noqa: B950
class MetricsCapture:
    def __init__(self) -> None:
        self.memory_reader = InMemoryMetricReader()
        self.meter_provider = MeterProvider(metric_readers=[self.memory_reader])
        self._metrics_data: t.Any = None

    @staticmethod
    def reset_provider() -> None:
        # Seems to be the only way to reset metrics api...
        internal_api._METER_PROVIDER_SET_ONCE = Once()
        internal_api._METER_PROVIDER = None
        internal_api._PROXY_METER_PROVIDER = internal_api._ProxyMeterProvider()

    def setup_provider(self) -> None:
        metrics_api.set_meter_provider(self.meter_provider)

    def get_metric(self, name: str) -> Metric:
        if self._metrics_data is None:
            self.collect()
        assert self._metrics_data
        resource_metrics = self._metrics_data.resource_metrics

        for metrics in resource_metrics:
            for scope_metrics in metrics.scope_metrics:
                for metric in scope_metrics.metrics:
                    if name == metric.name:
                        return metric
        raise IndexError(name)

    def collect(self) -> None:
        self._metrics_data = self.memory_reader.get_metrics_data()

    def assert_metric_expected(
        self,
        name: str,
        expected_data_points: t.Sequence[DataPointT],
        est_value_delta: float = 0,
    ) -> None:
        metric = self.get_metric(name)
        data_points: list[DataPointT] = list(metric.data.data_points)
        assert len(expected_data_points) == len(data_points)
        for expected_data_point in expected_data_points:
            self.assert_data_point_expected(
                expected_data_point, data_points, est_value_delta
            )

    @staticmethod
    def is_data_points_equal(
        expected_data_point: DataPointT,
        data_point: DataPointT,
        est_value_delta: float = 0,
    ) -> bool:
        if type(expected_data_point) is not type(data_point) or not isinstance(
            expected_data_point, (HistogramDataPoint, NumberDataPoint)
        ):
            return False

        values_diff = None
        if isinstance(data_point, NumberDataPoint):
            assert isinstance(expected_data_point, NumberDataPoint)
            values_diff = abs(expected_data_point.value - data_point.value)
        elif isinstance(data_point, HistogramDataPoint):
            assert isinstance(expected_data_point, HistogramDataPoint)
            values_diff = abs(expected_data_point.sum - data_point.sum)
            if expected_data_point.count != data_point.count or (
                est_value_delta == 0
                and (
                    expected_data_point.min != data_point.min
                    or expected_data_point.max != data_point.max
                )
            ):
                return False

        return (
            values_diff <= est_value_delta
            and expected_data_point.attributes == data_point.attributes
        )

    def assert_data_point_expected(
        self,
        expected_data_point: DataPointT,
        data_points: t.Sequence[DataPointT],
        est_value_delta: float = 0,
    ) -> None:
        is_data_point_exist = False
        for data_point in data_points:
            if self.is_data_points_equal(
                expected_data_point, data_point, est_value_delta
            ):
                is_data_point_exist = True
                break

        assert (
            is_data_point_exist
        ), f"Data point {expected_data_point} does not exist in {data_points}"

    @staticmethod
    def create_number_data_point(
        value: float, attributes: otel_types.Attributes
    ) -> NumberDataPoint:
        return NumberDataPoint(
            value=value,
            attributes=attributes,
            start_time_unix_nano=0,
            time_unix_nano=0,
        )

    @staticmethod
    def create_histogram_data_point(
        sum_data_point: float,
        count: int,
        max_data_point: float,
        min_data_point: float,
        attributes: otel_types.Attributes,
    ) -> HistogramDataPoint:
        return HistogramDataPoint(
            count=count,
            sum=sum_data_point,
            min=min_data_point,
            max=max_data_point,
            attributes=attributes,
            start_time_unix_nano=0,
            time_unix_nano=0,
            bucket_counts=[],
            explicit_bounds=[],
        )
