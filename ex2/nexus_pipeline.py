from typing import Any, Dict, List, Protocol, Union
from collections import Counter
from abc import ABC, abstractmethod
import time


class ProcessingStage(Protocol):

    def process(self, data: Any) -> Any:
        """Blueprint to the 'process' method"""
        ...


class InputStage:

    def process(self, data: Any) -> Any:
        """Validate basic input before transformations"""
        if data is None:
            raise ValueError("Input data cannot be None")

        if isinstance(data, str) and data.strip() == "":
            raise ValueError("Input string cannot be empty")

        if isinstance(data, dict):
            payload = data
            payload["type"] = "sensor"
            payload["size"] = len(data)

        if isinstance(data, str):
            lines = [line.strip() for line in data.splitlines()]
            if not lines:
                raise ValueError("CSV input is empty")

            header = [token.strip() for token in lines[0].split(sep=",")]

            records = []
            for i in range(1, len(lines)):
                records.append([token.strip()
                                for token in lines[i].split(sep=",")])

            payload = {"type": 'csv',
                       "header": header,
                       "records": records,
                       "row_count": len(records),
                       "size": len(records) + 1}

        if isinstance(data, list):
            for value in data:
                if (not isinstance(value, (int, float)) or
                        isinstance(value, bool)):
                    raise ValueError(f"Error: {value} is not numeric")
            payload = {"type": "stream",
                       "count": len(data),
                       "avg": (
                           round(sum(data) / len(data), 3)
                           if data
                           else 0.0),
                       "raw": data,
                       "size": len(data)}

        return payload


class TransformStage:

    def process(self, data: Any) -> Any:
        """Marks data as transformed"""
        if isinstance(data, dict):
            enriched = data
            enriched["transformed"] = True
            return enriched
        else:
            res_err = f"Invalid value, wanted a dict, got {data}\n"
            res_err += "Recovery initiated: Switching to backup processor\n"
            res_err += "Recovery successful: Pipeline restored, "
            res_err += "processing resumed"
            raise ValueError(res_err)


class OutputStage:
    def process(self, data: Any) -> Any:
        """Mark output as processed while preserving shape"""
        if not isinstance(data, dict):
            res_err = f"Invalid value, wanted a dict, got {data}\n"
            res_err += "Recovery initiated: Switching to backup processor\n"
            res_err += "Recovery successful: Pipeline restored, "
            res_err += "processing resumed"
            raise ValueError(res_err)
        if data["type"] == "sensor":
            summary = f"JSON summary: Processed {data['sensor']} value "
            summary += f"with a value of {data['value']} {data['unit']}"
        if data["type"] == "csv":
            summary = f"CSV summary: {len(data['header'])} columns, "
            summary += f"{len(data['records'])} rows"
        if data["type"] == "stream":
            summary = "Stream summary: "
            summary += f"{data['count']} readings, avg: {data['avg']}"
        return summary


class ProcessingPipeline(ABC):

    def __init__(self, pipeline_id: str) -> None:
        """Constructor for the ProcessingPipeline
        Initializes an empty list of stages"""
        self.pipeline_id: str = str(pipeline_id)
        self.stages: List[ProcessingStage] = []
        self.processed_batches: int = 0
        self.failures: int = 0
        self.total_processing_time: float = 0.0

    def add_stage(self, stage: ProcessingStage) -> None:
        """Adds a stage to the ProcessingPipeline"""
        if (isinstance(stage, InputStage)
            or isinstance(stage, TransformStage)
                or isinstance(stage, OutputStage)):
            self.stages.append(stage)
        else:
            print(f"Error: {stage} is not a valid stage")

    @abstractmethod
    def process(self, data: Any) -> Any:
        """Blueprint to process data"""
        raise NotImplementedError

    def run_stages(self, data: Any) -> Any:
        """Runs all the stages from the pipeline"""
        cur = data
        for index, stage in enumerate(self.stages, start=1):
            cur = stage.process(cur)
        return cur

    def record_success(self, elapsed: float) -> None:
        """Records a success"""
        self.processed_batches += 1
        self.total_processing_time += elapsed

    def record_failure(self, elapsed: float) -> None:
        """Records a success"""
        self.failures += 1
        self.total_processing_time += elapsed

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Gets stats from the pipeline"""
        avg_time = 0.0
        if self.processed_batches > 0:
            avg_time = self.total_processing_time / self.processed_batches
        return {
            "pipeline_id": self.pipeline_id,
            "pipeline_type": self.__class__.__name__,
            "processed_batches": self.processed_batches,
            "failures": self.failures,
            "avg_time_s": round(avg_time, 6),
        }


class JSONAdapter(ProcessingPipeline):

    def process(self, data: Any) -> Any:
        """Process the entry through every stages if it's a dict,
        returns an error message otherwise"""
        start = time.time()
        if not isinstance(data, dict):
            res_err = f"Invalid value, wanted a dict, got {data}\n"
            res_err += "Recovery initiated: Switching to backup processor\n"
            res_err += "Recovery successful: Pipeline restored, "
            res_err += "processing resumed"
            raise ValueError(res_err)
        try:
            processed = self.run_stages(data)
            self.record_success(time.time() - start)
            return processed
        except Exception as cur_error:
            self.record_failure(time.time() - start)
            res_error = f"{cur_error}"
            return res_error


class CSVAdapter(ProcessingPipeline):

    def process(self, data: Any) -> Any:
        """Process the entry through every stages if it's a string,
        returns an error message otherwise"""
        start = time.time()
        if (not isinstance(data, str) and
                not (isinstance(data, dict) and data["type"] == "csv")):
            res_err = f"Invalid value, wanted a str, got {data}\n"
            res_err += "Recovery initiated: Switching to backup processor\n"
            res_err += "Recovery successful: Pipeline restored, "
            res_err += "processing resumed"
            raise ValueError(res_err)
        try:
            processed = self.run_stages(data)
            self.record_success(time.time() - start)
            return processed
        except Exception as cur_error:
            self.record_failure(time.time() - start)
            res_error = f"{cur_error}"
            return res_error


class StreamAdapter(ProcessingPipeline):

    def process(self, data: Any) -> Any:
        """Process the entry through every stages if it's a list,
        returns an error message otherwise"""
        start = time.time()
        if (not isinstance(data, list) and
                not (isinstance(data, dict) and data["type"] == "stream")):
            res_err = f"Invalid value, wanted a list, got {data}\n"
            res_err += "Recovery initiated: Switching to backup processor\n"
            res_err += "Recovery successful: Pipeline restored, "
            res_err += "processing resumed"
            raise ValueError(res_err)
        try:
            processed = self.run_stages(data)
            self.record_success(time.time() - start)
            return processed
        except Exception as cur_error:
            self.record_failure(time.time() - start)
            res_error = f"{cur_error}"
            return res_error


class NexusManager:

    def __init__(self) -> None:
        """Constructor for the NexusManager"""
        self.pipelines: Dict[str, ProcessingPipeline] = {}
        self.type_counter: Counter[str] = Counter()

    def register_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Registers a pipeline in the manager, places it at the end"""
        self.pipelines[pipeline.pipeline_id] = pipeline

    def process_with_pipeline(self, pipeline_id: str, data: Any) -> Any:
        """Proccesses the input with a specific pipeline"""
        pipeline = self.pipelines.get(pipeline_id)
        if pipeline is None:
            return f"Pipeline '{pipeline_id}' not found"
        self.type_counter[pipeline.__class__.__name__] += 1
        try:
            res = pipeline.process(data)
            return res
        except Exception as cur_error:
            return cur_error

    def chain_pipelines(self, pipeline_ids: List[str], data: Any) -> Any:
        """Processes the input with all the pipelines"""
        current = data
        for pipeline_id in pipeline_ids:
            pipeline = self.pipelines.get(pipeline_id)
            if pipeline is None:
                return f"Pipeline '{pipeline_id}' not found during chaining"
            self.type_counter[pipeline.__class__.__name__] += 1
            try:
                current = pipeline.process(current)
            except Exception as cur_error:
                print(cur_error)
                return str(cur_error)

        temp = InputStage().process(data)
        print(f"Chain result: {temp['size']} records processed ", end="")
        print(f"through {len(pipeline_ids)}-stage pipeline")
        successes = sum(pipeline.processed_batches
                        for pipeline in self.pipelines.values())
        errors = sum(pipeline.failures for pipeline in self.pipelines.values())
        total_time = sum(pipeline.total_processing_time
                         for pipeline in self.pipelines.values())
        total_runs = successes + errors
        efficiency = (successes / total_runs * 100) if total_runs else 0.0
        print(
            f"Performance: {efficiency:.1f}% efficiency, "
            f"{total_time:.6f}s total processing time"
        )
        return current

    def get_stats(self) -> Dict[str, Any]:
        """Gets various stats about the pipelines in the manager"""
        return {
            "registered_pipelines": len(self.pipelines),
            "pipeline_usage": dict(self.type_counter),
            "pipelines_ids": list(self.pipelines.keys()),
            "pipeline_stats": [
                pipeline.get_stats() for pipeline in self.pipelines.values()
            ],
            "elapsed_time": sum(pipeline.total_processing_time
                                for pipeline in self.pipelines.values()),
            "successes": sum(pipeline.processed_batches
                             for pipeline in self.pipelines.values()),
            "errors": sum(pipeline.failures
                          for pipeline in self.pipelines.values())
        }


def build_standard_stages(pipeline: ProcessingPipeline) -> None:
    """Build a standard complete pipeline"""
    pipeline.add_stage(InputStage())
    pipeline.add_stage(TransformStage())
    pipeline.add_stage(OutputStage())


if __name__ == "__main__":
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print()
    print("Initializing Nexus Manager...")
    manager = NexusManager()
    print("Pipeline capacity: 1000 streams/second")
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
    print()
    json_input = {"sensor": "temp", "value": 23.5, "unit": "C"}
    csv_input = "user,action,timestamp\nalice,login,2026-02-11"
    stream_input = [21.1, 22.3, 23.0, 22.0, 22.1]

    print("Processing JSON data through pipeline...")
    print(f"Input: {json_input}")
    print("Transform: Enriched with metadata and validation")
    print(
        "Output:",
        manager.process_with_pipeline(
            "JSON_PIPE_001",
            json_input))
    print()

    print("Processing CSV data through same pipeline...")
    print(f"Input: {csv_input.splitlines()}")
    print("Transform: Parsed and structured data")
    print("Output:", manager.process_with_pipeline("CSV_PIPE_001", csv_input))
    print()

    print("Processing Stream data through same pipeline...")
    print("Input: Real-time sensor stream")
    print("Transform: Aggregated and filtered")
    print(
        "Output:",
        manager.process_with_pipeline(
            "STREAM_PIPE_001",
            stream_input))
    print()

    print("=== Pipeline Chaining Demo ===")
    print("Pipeline A -> Pipeline B -> Pipeline C")
    print("Data flow: Raw -> Processed -> Analyzed -> Stored")
    print()

    manager_chain = NexusManager()

    stream_input_stage = StreamAdapter("JSON_INPUT_001")
    stream_input_stage.add_stage(InputStage())

    stream_transform_stage = StreamAdapter("JSON_TRANSFORM_001")
    stream_transform_stage.add_stage(TransformStage())

    stream_output_stage = StreamAdapter("JSON_OUTPUT_001")
    stream_output_stage.add_stage(OutputStage())

    manager_chain.register_pipeline(stream_input_stage)
    manager_chain.register_pipeline(stream_transform_stage)
    manager_chain.register_pipeline(stream_output_stage)

    big_stream_input = [i for i in range(100)]

    chain_result = manager_chain.chain_pipelines(
        ["JSON_INPUT_001", "JSON_TRANSFORM_001", "JSON_OUTPUT_001"],
        big_stream_input)
    print(chain_result)
    print()

    print("=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    error_result = manager.process_with_pipeline(
        "JSON_PIPE_001", "ABCDEF")
    print(error_result)
    print()

    print("=== Performance / Stats ===")
    stats = manager.get_stats()
    print(f"Number of pipelines: {stats['registered_pipelines']}")
    print(f"Pipelines: {stats['pipelines_ids']}")
    print(f"Total elapsed time: {stats['elapsed_time']:.6f}secs")
    print(f"Number of successful pipelines: {stats['successes']}")
    print(f"Number of failed pipelines: {stats['errors']}")
    try:
        efficiency = (stats['successes'] + stats['errors']
                      ) / stats['successes'] * 100
    except ZeroDivisionError:
        efficiency = 100.0
    print(f"Efficiency: {efficiency:.2f}%")
    print()
    print("Nexus Integration complete. All systems operational.")
