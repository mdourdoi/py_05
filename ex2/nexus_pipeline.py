from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Protocol
import collections


class ProcessingStage(Protocol):

    def process(self, data: Any) -> Any:
        """Blueprint to the 'process' method"""
        ...


class InputStage:

    def process(self, data: Any) -> Any:
        """Input data of different types"""
        ...


class TransformStage:

    def process(self, data: Any) -> Any:
        """Process data"""
        ...


class OutputStage:

    def process(self, data: Any) -> Any:
        """Output processed data"""
        ...


class ProcessingPipeline(ABC):

    def __init__(self) -> None:
        """Constructor for the ProcessingPipeline
        Initializes an empty list of stages"""
        self.__stages = []

    def add_stage(self, stage: ProcessingStage) -> None:
        """Adds a stage to the ProcessingPipeline"""
        if (isinstance(stage, self.InputStage)
            or isinstance(stage, self.TransformStage)
                or isinstance(stage, self.OutputStage)):
            self.__stage.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        raise NotImplementedError


class JSONAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:


class CSVAdapter(ProcessingPipeline):

    def process(self, data: Any) -> Union[str, Any]:


class StreamAdapter(ProcessingPipeline):

    def process(self, data: Any) -> Union[str, Any]:
















from __future__ import annotations

from abc import ABC, abstractmethod
from collections import Counter
from typing import Any, Dict, List, Protocol, Union
import json
import time


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        """Duck-typed interface: any stage must implement process()."""


class InputStage:
    def process(self, data: Any) -> Any:
        """Validate basic input quality before transformations."""
        if data is None:
            raise ValueError("Input data cannot be None")
        if isinstance(data, str) and data.strip() == "":
            raise ValueError("Input string cannot be empty")
        return data


class TransformStage:
    def process(self, data: Any) -> Any:
        """Apply lightweight enrichment/transformation."""
        if isinstance(data, dict):
            enriched = dict(data)
            enriched["transformed"] = True
            return enriched
        if isinstance(data, list):
            return [
                item.strip() if isinstance(item, str) else item
                for item in data
            ]
        if isinstance(data, str):
            return data.strip()
        return data


class OutputStage:
    def process(self, data: Any) -> Any:
        """Mark output as processed while preserving payload shape."""
        if isinstance(data, dict):
            output = dict(data)
            output["output_ready"] = True
            return output
        return data


class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id: str) -> None:
        self.pipeline_id: str = pipeline_id
        self._stages: List[ProcessingStage] = []
        self._processed_batches: int = 0
        self._failures: int = 0
        self._total_processing_time: float = 0.0

    def add_stage(self, stage: ProcessingStage) -> None:
        self._stages.append(stage)

    def run_stages(self, data: Any) -> Any:
        current = data
        for stage in self._stages:
            current = stage.process(current)
        return current

    def _record_success(self, elapsed: float) -> None:
        self._processed_batches += 1
        self._total_processing_time += elapsed

    def _record_failure(self, elapsed: float) -> None:
        self._failures += 1
        self._total_processing_time += elapsed

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        avg_time = 0.0
        if self._processed_batches > 0:
            avg_time = self._total_processing_time / self._processed_batches
        return {
            "pipeline_id": self.pipeline_id,
            "pipeline_type": self.__class__.__name__,
            "processed_batches": self._processed_batches,
            "failures": self._failures,
            "avg_time_s": round(avg_time, 6),
        }

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        raise NotImplementedError


class JSONAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        start = time.perf_counter()
        try:
            if isinstance(data, str):
                payload: Any = json.loads(data)
            elif isinstance(data, dict):
                payload = data
            else:
                raise TypeError("JSONAdapter expects dict or JSON string")

            processed = self.run_stages(payload)
            result = json.dumps(processed, separators=(",", ":"))
            self._record_success(time.perf_counter() - start)
            return result
        except Exception as exc:
            self._record_failure(time.perf_counter() - start)
            return f"[RECOVERY:{self.pipeline_id}] JSON processing failed: {exc}"


class CSVAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        start = time.perf_counter()
        try:
            if not isinstance(data, str):
                raise TypeError("CSVAdapter expects CSV text input")

            lines = [line.strip()
                     for line in data.splitlines() if line.strip()]
            if not lines:
                raise ValueError("CSV input is empty")

            header = [token.strip() for token in lines[0].split(",")]
            records: List[Dict[str, str]] = []
            for line in lines[1:]:
                values = [token.strip() for token in line.split(",")]
                row = {
                    header[index]: values[index] if index < len(values) else ""
                    for index in range(len(header))
                }
                records.append(row)

            payload = {
                "header": header,
                "records": records,
                "row_count": len(records),
            }
            processed = self.run_stages(payload)
            summary = (
                "CSV summary: "
                f"{len(header)} columns, {payload['row_count']} rows"
            )
            self._record_success(time.perf_counter() - start)
            return f"{summary} | output={processed}"
        except Exception as exc:
            self._record_failure(time.perf_counter() - start)
            return f"[RECOVERY:{self.pipeline_id}] CSV processing failed: {exc}"


class StreamAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        start = time.perf_counter()
        try:
            stream_values: List[Any]
            if isinstance(data, list):
                stream_values = data
            elif isinstance(data, dict):
                readings = data.get("readings")
                if isinstance(readings, list):
                    stream_values = readings
                else:
                    stream_values = list(data.values())
            elif isinstance(data, str):
                stream_values = [len(token) for token in data.split() if token]
            else:
                raise TypeError(
                    "StreamAdapter expects list, dict or string input")

            numeric_values = [
                value for value in stream_values
                if isinstance(value, (int, float)) and not isinstance(value, bool)
            ]

            payload: Dict[str, Any] = {
                "count": len(stream_values),
                "numeric_count": len(numeric_values),
                "avg": (
                    round(sum(numeric_values) / len(numeric_values), 3)
                    if numeric_values
                    else 0.0
                ),
                "raw": stream_values,
            }
            processed = self.run_stages(payload)
            self._record_success(time.perf_counter() - start)
            return (
                "Stream summary: "
                f"{payload['count']} readings, avg={payload['avg']}"
                f" | output={processed}"
            )
        except Exception as exc:
            self._record_failure(time.perf_counter() - start)
            return f"[RECOVERY:{self.pipeline_id}] Stream processing failed: {exc}"


class NexusManager:
    def __init__(self) -> None:
        self._pipelines: Dict[str, ProcessingPipeline] = {}
        self._type_counter: Counter[str] = Counter()

    def register_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self._pipelines[pipeline.pipeline_id] = pipeline

    def process_with_pipeline(self, pipeline_id: str,
                              data: Any) -> Union[str, Any]:
        pipeline = self._pipelines.get(pipeline_id)
        if pipeline is None:
            return f"Pipeline '{pipeline_id}' not found"
        self._type_counter[pipeline.__class__.__name__] += 1
        return pipeline.process(data)

    def chain_pipelines(
            self, pipeline_ids: List[str], data: Any) -> Union[str, Any]:
        current = data
        for pipeline_id in pipeline_ids:
            pipeline = self._pipelines.get(pipeline_id)
            if pipeline is None:
                return f"Pipeline '{pipeline_id}' not found during chaining"
            self._type_counter[pipeline.__class__.__name__] += 1
            current = pipeline.process(current)
        return current

    def get_stats(self) -> Dict[str, Any]:
        return {
            "registered_pipelines": len(self._pipelines),
            "pipeline_usage": dict(self._type_counter),
            "pipeline_stats": [
                pipeline.get_stats() for pipeline in self._pipelines.values()
            ],
        }


def build_standard_stages(pipeline: ProcessingPipeline) -> None:
    pipeline.add_stage(InputStage())
    pipeline.add_stage(TransformStage())
    pipeline.add_stage(OutputStage())


if __name__ == "__main__":
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print("Initializing Nexus Manager...")
    manager = NexusManager()
    print("Pipeline capacity: polymorphic multi-format processing")
    print()

    print("Creating Data Processing Pipeline...")
    json_adapter = JSONAdapter("JSON_PIPE_001")
    csv_adapter = CSVAdapter("CSV_PIPE_001")
    stream_adapter = StreamAdapter("STREAM_PIPE_001")

    build_standard_stages(json_adapter)
    build_standard_stages(csv_adapter)
    build_standard_stages(stream_adapter)

    manager.register_pipeline(json_adapter)
    manager.register_pipeline(csv_adapter)
    manager.register_pipeline(stream_adapter)

    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery")
    print()

    print("=== Multi-Format Data Processing ===")
    json_input = '{"sensor":"temp","value":23.5,"unit":"C"}'
    csv_input = "user,action,timestamp\nalice,login,2026-02-11"
    stream_input = [21.1, 22.3, 23.0, 22.0, 22.1]

    print("Processing JSON data through pipeline...")
    print(f"Input: {json_input}")
    print(
        "Output:",
        manager.process_with_pipeline(
            "JSON_PIPE_001",
            json_input))
    print()

    print("Processing CSV data through same pipeline...")
    print(f"Input: {csv_input.splitlines()[0]}")
    print("Output:", manager.process_with_pipeline("CSV_PIPE_001", csv_input))
    print()

    print("Processing Stream data through same pipeline...")
    print("Input: Real-time sensor stream")
    print(
        "Output:",
        manager.process_with_pipeline(
            "STREAM_PIPE_001",
            stream_input))
    print()

    print("=== Pipeline Chaining Demo ===")
    print("Pipeline A -> Pipeline B -> Pipeline C")
    print("Data flow: Raw -> Processed -> Analyzed -> Stored")
    chain_result = manager.chain_pipelines(
        ["JSON_PIPE_001", "CSV_PIPE_001", "STREAM_PIPE_001"],
        '{"readings":[20.1,21.4,22.0]}',
    )
    print("Chain result:", chain_result)
    print()

    print("=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    error_result = manager.process_with_pipeline("JSON_PIPE_001", "{bad json")
    print(error_result)
    print("Recovery initiated: fallback message emitted")
    print("Recovery successful: pipeline remains operational")
    print()

    print("=== Performance / Stats ===")
    stats = manager.get_stats()
    print(stats)
    print()
    print("Nexus Integration complete. All systems operational.")
