from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):

    def __init__(self, stream_id: str) -> None:
        """Basic constructor for data streams"""
        self.__stream_id = str(stream_id)
        self.__count = 0
        self.__errors = 0

    def get_stream_id(self) -> str:
        """Gets the stream id"""
        return self.__stream_id

    def get_count(self) -> int:
        """Gets the count"""
        return self.__count

    def add_count(self, value: int) -> None:
        """Add to the count"""
        if isinstance(value, int):
            self.__count += value
        else:
            print(f"Error, {value} is not a number, nothing was done")

    def get_errors(self) -> int:
        """Gets the errors"""
        return self.__errors

    def add_errors(self, value: int) -> None:
        """Gets the count"""
        if isinstance(value, int):
            self.__errors += value
        else:
            print(f"Error, {value} is not a number, nothing was done")

    def reset_count(self) -> None:
        """Reset the count"""
        self.__count = 0

    def reset_errors(self) -> None:
        """Reset the errors"""
        self.__errors = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Blueprint : processes a batch of data"""
        raise NotImplementedError

    def filter_data(self, data_batch: List[Any], criteria: Optional[str]
                    = None) -> List[Any]:
        """Filter data based on criteria
        Probably useless and needs to be overriden"""
        if criteria is None:
            return data_batch
        return [value for value in data_batch if criteria.lower()
                in str(value).lower()]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics"""
        return {"stream_id": self.get_stream_id(),
                "stream_type": "generic",
                "processed_count": self.get_count(),
                "errors": self.get_errors()}


class SensorStream(DataStream):

    @staticmethod
    def validate_input(data: List[Any]) -> bool:
        """Validates that the data is a sensor batch"""
        if isinstance(data, list):
            for value in data:
                if value is None or value == "":
                    return False
                temp = value.split(sep=":")
                if len(temp) != 2:
                    return False
                if temp[0] not in ["temp", "humidity", "pressure"]:
                    return False
                try:
                    float(temp[1])
                except ValueError:
                    return False
            return True
        return False

    def process_batch(self, data_batch: List[Any]) -> str:
        """Validates adf processes the batch"""
        if self.validate_input(data_batch):
            ret = f"Stream ID: {self.get_stream_id()}, "
            ret += "Type = Environmental Data\n"
            total_temp = 0
            temp_batch = self.filter_data(data_batch, "temp")
            for value in temp_batch:
                total_temp += float(value.split(sep=":")[1])
                if float(value.split(sep=":")[1]) > 100:
                    self.add_errors(1)
            ret += f"Processing sensor batch: {data_batch}\n"
            ret += f"Sensor analysis: {len(data_batch)} reading processed, "
            self.add_count(len(data_batch))
            if len(temp_batch) == 0:
                ret += "no temperature input"
            else:
                ret += f"avg temp: {total_temp / len(temp_batch)}"
            return ret
        else:
            return "Invalid data batch"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics"""
        return {"stream_id": self.get_stream_id(),
                "stream_type": "sensor",
                "processed_count": self.get_count(),
                "errors": self.get_errors()}


class TransactionStream(DataStream):

    @staticmethod
    def validate_input(data: List[Any]) -> bool:
        """Validates that the data is a transaction batch"""
        if isinstance(data, list):
            for value in data:
                if value is None or value == "":
                    return False
                temp = value.split(sep=":")
                if len(temp) != 2:
                    return False
                if temp[0] not in ["buy", "sell"]:
                    return False
                try:
                    int(temp[1])
                except ValueError:
                    return False
            return True
        return False

    def process_batch(self, data_batch: List[Any]) -> str:
        """Validates adf processes the batch"""
        if self.validate_input(data_batch):
            ret = f"Stream ID: {self.get_stream_id()}, "
            ret += "Type = Financial Data\n"
            net_flow = 0
            for value in data_batch:
                if value.split(sep=":")[0] == "buy":
                    net_flow += int(value.split(sep=":")[1])
                else:
                    net_flow -= int(value.split(sep=":")[1])
                if int(value.split(sep=":")[1]) > 1000:
                    self.add_errors(1)
            ret += f"Processing transaction batch: {data_batch}\n"
            ret += f"Transaction analysis: {len(data_batch)} operations, "
            self.add_count(len(data_batch))
            ret += "net flow: "
            if net_flow > 0:
                ret += f"+{net_flow} units"
            else:
                ret += f"{net_flow} units"
            return ret
        else:
            return "Invalid data batch"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics"""
        return {"stream_id": self.get_stream_id(),
                "stream_type": "transaction",
                "processed_count": self.get_count(),
                "errors": self.get_errors()}


class EventStream(DataStream):

    @staticmethod
    def validate_input(data: List[Any]) -> bool:
        """Validates that the data is a transaction batch"""
        if isinstance(data, list):
            for value in data:
                if value not in ["login", "error", "logout"]:
                    return False
            return True
        return False

    def process_batch(self, data_batch: List[Any]) -> str:
        """Validates adf processes the batch"""
        if self.validate_input(data_batch):
            ret = f"Stream ID: {self.get_stream_id()}, "
            ret += "Type = System Events\n"
            errors_batch = self.filter_data(data_batch, "error")
            self.add_errors(len(errors_batch))
            ret += f"Processing events batch: {data_batch}\n"
            self.add_count(len(data_batch))
            ret += f"Event analysis: {len(data_batch)} events, "
            if len(errors_batch) > 1:
                ret += f"{len(errors_batch)} errors detected"
            if len(errors_batch) == 1:
                ret += f"{len(errors_batch)} error detected"
            else:
                ret += "no error detected"
            return ret
        else:
            return "Invalid data batch"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics"""
        return {"stream_id": self.get_stream_id(),
                "stream_type": "event",
                "processed_count": self.get_count(),
                "errors": self.get_errors()}


class StreamProcessor:

    def __init__(self) -> None:
        """Constructor for DataStreams"""
        self.sensor_error = 0
        self.transaction_error = 0
        self.event_error = 0

    def batch_processing(self, *stream_batch: DataStream) -> str:
        """Processes a batch of DataStream"""
        ret = ""
        for stream in stream_batch:
            if isinstance(stream, SensorStream):
                ret += f"Sensor data: {stream.get_count()} readings processed\n"
                self.sensor_error += stream.get_stats()["errors"]
            if isinstance(stream, TransactionStream):
                ret += f"Transaction data: {stream.get_count()} "
                ret += "operations processed\n"
                self.transaction_error += stream.get_stats()["errors"]
            if isinstance(stream, EventStream):
                ret += f"Event data: {stream.get_count()} events processed \n"
                self.event_error += stream.get_stats()["errors"]
            if not isinstance(stream, DataStream):
                return "At least one value is not a DataStream"
        ret += "\n"
        ret += "Stream filtering active: High-priority data only\n"
        ret += "Filtered results : "
        if self.sensor_error:
            ret += f"{self.sensor_error} critical sensor alerts, "
        if self.transaction_error:
            ret += f"{self.transaction_error} large transactions, "
        if self.event_error:
            ret += f"{self.event_error} events error"
        return ret


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()

    print("Initializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(sensor.process_batch(["temp:22.5", "humidity:65", "pressure:1013"]))
    print()

    print("Initializing Transaction Stream...")
    transaction = TransactionStream("TRANS_001")
    print(transaction.process_batch(["buy:100", "sell:150", "buy:75"]))
    print()

    print("Initializing Transaction Stream...")
    event = EventStream("EVENT_001")
    print(event.process_batch(["login", "error", "logout"]))
    print()

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface")
    stream_proc = StreamProcessor()
    sensor_data = ["temp:2000002.5", "temp:2000002.5"]
    transaction_data = ["buy:100", "sell:150", "buy:75", "sell:1000000"]
    event_data = ["login", "error", "logout"]
    sensor_stream = SensorStream("SENSOR_002")
    transaction_stream = TransactionStream("TRANS_002")
    event_stream = EventStream("EVENT_002")
    print()
    sensor_stream.process_batch(sensor_data)
    transaction_stream.process_batch(transaction_data)
    event_stream.process_batch(event_data)
    print("Batch 1 Results:")
    print(
        stream_proc.batch_processing(
            sensor_stream,
            transaction_stream,
            event_stream))
    print()
    print("All streams processed successfully. Nexus throughput optimal.")
