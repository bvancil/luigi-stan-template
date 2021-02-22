"""Extensions of Luigi components"""

# Built-in libraries
import os

# PyPI libraries
import luigi
import pandas
from pyarrow import Table
from pyarrow.parquet import read_table
from pyarrow.parquet import write_table

# Helpful functions

# Generic formats
# def PandasParquetFormat(luigi.Format):
#     input = 'bytes'
#     output = 'bytes'

#     def pipe_reader(self, input_pipe):
#         pass

#     def pipe_writer(self, output_pipe):
#         pass

# Generic targets
class ParquetTarget(luigi.LocalTarget):
    def __init__(self, path=None, format=None, is_tmp=False):
        if not path:
            if not is_tmp:
                raise Exception('path or is_tmp must be set')
        else:
            path = f"./data/{path}.parquet"
        super(ParquetTarget, self).__init__(path, format=format, is_tmp=is_tmp)

    def read_path(self):
        return read_table(self.path).to_pandas()

    def write_path(self, dataset):
        table = Table.from_pandas(dataset)
        write_table(table, self.path)

# Generic tasks
class TimewiseCompletedTask(luigi.Task):
    def complete(self):
        # See https://stackoverflow.com/questions/28793832/can-luigi-rerun-tasks-when-the-task-dependencies-become-out-of-date
        def mtime(path):
            return os.path.getmtime(path)

        if not super().complete():
            return False
        
        output_mtime=mtime(self.output().path)
        def up_to_date(required_task):
            return required_task.complete() and all(
                mtime(output.path) < output_mtime
                for output in luigi.task.flatten(required_task.output())
            )

        # breakpoint()

        return all(
            up_to_date(required_task)
            for required_task in luigi.task.flatten(self.requires())
        )

class SourceTask(TimewiseCompletedTask, luigi.ExternalTask):
    def output(self):
        raise NotImplementedError(f'You might have forgotten to override output() when subclassing {type(self).__name__}')       

class ExtractTask(TimewiseCompletedTask):
    """
    Run a given function to extract to a given file
    """
    def function(self, *args) -> pandas.DataFrame:
        raise NotImplementedError(f'You might have forgotten to override function() when subclassing {type(self).__name__}')

    def run(self):
        self.output().write_path(self.function())

class TransformTask(TimewiseCompletedTask):
    """
    Run a given function to transform from a give set of
    files specified by a dictionary to another given file
    """
    def function(self, *args) -> pandas.DataFrame:
        raise NotImplementedError(f'You might have forgotten to override function() when subclassing {type(self).__name__}')
    
    def input_datasets(self):
        return (
            target.read_path()
            for target in self.input()
            if hasattr(target, "read_path")
        )

    def run(self):
        output = self.function(*self.input_datasets())
        self.output().write_path(output)
