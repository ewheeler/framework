from __future__ import annotations
from ...platform import platform
from ...resources import TableResource
from .control import ParquetControl
from ...system import Parser
from .. import settings


class ParquetParser(Parser):
    """JSONL parser implementation."""

    supported_types = [
        "array",
        "boolean",
        "datetime",
        "date",
        "duration",
        "integer",
        "number",
        "object",
        "string",
        "time",
    ]

    # Read

    def read_cell_stream_create(self):
        # params are different for pyarrow compared to fastparquet
        # TODO define comparable ParquetControl for pyarrow
        control = ParquetControl.from_dialect(self.resource.dialect)
        handle = self.resource.normpath
        if self.resource.remote:
            handles = platform.pandas.io.common.get_handle(  # type: ignore
                self.resource.normpath, "rb", is_text=False
            )
            handle = handles.handle
        if 'pyarrow' == settings.PARQUET_ENGINE:
            file = platform.pyarrow.parquet.ParquetFile(handle)
            # TODO pass in `control` params to `iter_batches` and/or `to_pandas`
            for group, df in enumerate(file.iter_batches(), start=1):
                with TableResource(data=df.to_pandas(), format="pandas") as resource:
                    for line, cells in enumerate(resource.cell_stream, start=1):
                        # Starting from second group we don't need a header row
                        if group != 1 and line == 1:
                            continue
                        yield cells
        if 'fastparquet' == settings.PARQUET_ENGINE:
            file = platform.fastparquet.ParquetFile(handle)
            for group, df in enumerate(file.iter_row_groups(**control.to_python()), start=1):
                with TableResource(data=df, format="pandas") as resource:
                    for line, cells in enumerate(resource.cell_stream, start=1):
                        # Starting from second group we don't need a header row
                        if group != 1 and line == 1:
                            continue
                        yield cells

    # Write

    def write_row_stream(self, source):
        if 'pyarrow' == settings.PARQUET_ENGINE:
            platform.pyarrow.parquet.write_to_dataset(platform.pyarrow.Table.from_pandas(source.to_pandas()),
                                                      root_path=self.resource.normpath)
        if 'fastparquet' == settings.PARQUET_ENGINE:
            platform.fastparquet.write(self.resource.normpath, source.to_pandas())
