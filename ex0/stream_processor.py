from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        """Blueprint for processing data"""
        raise NotImplementedError

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Blueprint for data validation"""
        raise NotImplementedError

    def format_output(self, result: str) -> str:
        """Method that displays a basic message, made to be overriden"""
        return "Output: This is the default DataProcessor output"


class NumericProcessor(DataProcessor):
    @staticmethod
    def check_numeric(value: Any) -> None:
        """Raises an error if the value is not an in or a float"""
        if (isinstance(value, bool)
                or (not isinstance(value, int)
                    and not isinstance(value, float))):
            raise Exception(f"{value} is not a numeric value")

    def validate(self, data: Any) -> bool:
        """Validates that data is a list of int or float"""
        if isinstance(data, list):
            for i in range(len(data)):
                try:
                    self.check_numeric(data[i])
                except Exception:
                    return False
            return True
        return False

    def process(self, data: Any) -> str:
        """Processes numeric lists only"""
        if self.validate(data):
            ret = f"Processed {len(data)} values, "
            ret += f"sum={sum(data)}, avg={sum(data) / len(data)}"
        else:
            ret = "Error during processing: "
            ret += f"{data} is not a list of integers"
        return ret

    def format_output(self, result: str) -> str:
        """Formats and displays the output"""
        print(f"Processing data: {result}")
        if self.validate(result):
            ret = "Validation: Numeric data verified\nOutput: "
            ret += self.process(result)
            return ret
        else:
            return "Invalid data type.Aborting..."


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        """Validates that data is a string"""
        if isinstance(data, str):
            return True
        return False

    def process(self, data: Any) -> str:
        """Processes numeric lists only"""
        if self.validate(data):
            ret = "Processed text: "
            ret += f"{len(data)} characters, {len(data.split())} words"
        else:
            ret = "Error during processing: "
            ret += f"{data} is not a text input"
        return ret

    def format_output(self, result: str) -> str:
        """Formats and displays the output"""
        print(f"Processing data: \"{result}\"")
        if self.validate(result):
            ret = "Validation: Text data verified\nOutput: "
            ret += self.process(result)
            return ret
        else:
            return "Invalid data type. Aborting..."


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        """Validates that data is a string formated as a log"""
        if isinstance(data, str):
            if data[:6] == "ERROR:" or data[:5] == "INFO:":
                return True
        return False

    def process(self, data: Any) -> str:
        """Processes numeric lists only"""
        if self.validate(data):
            if data[:6] == "ERROR:":
                ret = "[ALERT] ERROR level detected:"
                ret += f"{data[6:]}"
            if data[:5] == "INFO:":
                ret = "[INFO] INFO level detected:"
                ret += f"{data[5:]}"
        else:
            ret = "Error during processing: "
            ret += f"{data} is not a log input"
        return ret

    def format_output(self, result: str) -> str:
        """Formats and displays the output"""
        print(f"Processing data: \"{result}\"")
        if self.validate(result):
            ret = "Validation: Numeric data verified\nOutput: "
            ret += self.process(result)
            return ret
        else:
            return "Invalid data type. Aborting..."


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()

    print("Initializing Numeric Processor...")
    num_proc = NumericProcessor()
    print(num_proc.format_output([1, 2, 3, 4, 5]))
    print()

    print("Initializing Text Processor...")
    text_proc = TextProcessor()
    print(text_proc.format_output("Hello Nexus World"))
    print()

    print("Initializing Log Processor...")
    log_proc = LogProcessor()
    print(log_proc.format_output("ERROR: Connection timeout"))
    print()

    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    processors = [NumericProcessor(), TextProcessor(), LogProcessor()]
    inputs = [[1, 2, 3], "Hello Nexus", "INFO: System ready"]

    for i in range(3):
        print(f"Result {i + 1}:", processors[i].process(inputs[i]))
    print()
    print("Foundation systems online. Nexus ready for advanced streams")
