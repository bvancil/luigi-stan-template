# Built-in libraries
import datetime

# PyPI libraries
import luigi

# Project libraries
import functions
from toad import SourceTask
from toad import ExtractTask
from toad import TransformTask
from toad import ParquetTarget

# DAG specification

class FunctionsSource(SourceTask):
    def output(self):
        return luigi.LocalTarget('functions.py')

class SimulatedDataset(ExtractTask):
    def function(self):
        return functions.get_simulated_dataset()
    def requires(self):
        return [FunctionsSource()]
    def output(self):
        return ParquetTarget('extracted/simulated_dataset')

class TransformedDataset(TransformTask):
    def function(self, dataset):
        return functions.get_transformed_dataset(dataset)
    def requires(self):
        return [SimulatedDataset(), FunctionsSource()]
    def output(self):
        return ParquetTarget('working/transformed_dataset')

class All(luigi.WrapperTask):
    timestamp = luigi.DateSecondParameter(default=datetime.datetime.now())

    def run(self):
        print("Running All")
    
    def requires(self):
        return TransformedDataset()

if __name__ == '__main__':
    # luigi.run() will let you pass in parameters
    luigi_run_result = luigi.build(
        [All()], 
        local_scheduler=True, 
        detailed_summary=True
    )
    print(luigi_run_result.summary_text)
