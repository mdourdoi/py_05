from abc import ABC
from typing import Any, List, Dict, Union, Protocol


class ProcessingStage(Protocol):

    def process(self, data: Any) -> Any:
        """Blueprint to the 'process' method"""
        ...


class InputStage:

    def process(self, data: Any) -> Dict:
        """Blueprint to input data"""
        ...


class TransformStage:

    def process(self, data: Any) -> Dict:
        """Blueprint to process data"""
        ...


class OutputStage:

    def process(self, data: Any) -> str:
        """Blueprint to output processed data"""
        ...


class ProcessingPipeline(ABC):

    def __init__(self) -> None:
        """Constructor for the ProcessingPipeline
        Initializes an empty list of stages"""
        self.__stages = []

    def add_stage(self,
                  stage: Union[InputStage,
                               TransformStage,
                               OutputStage]) -> None:
        """Adds a stage to the ProcessingPipeline"""
        if (isinstance(stage, self.InputStage)
            or isinstance(stage, self.TransformStage)
                or isinstance(stage, self.OutputStage)):
            self.__stage.append(stage)
