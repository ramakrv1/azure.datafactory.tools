import pytest

from ingest.adapters.spark_common import SparkReader
from ingest.core.model import Source, TableRef
from tests.spark.conftest import assert_rows

pytestmark = pytest.mark.spark


def test_reads_csv_with_options(spark, tmp_path):
    (tmp_path / "a.csv").write_text("id,v\n1,a\n2,b\n", encoding="utf-8")
    src = Source("files", format="csv", path=str(tmp_path), options={"header": "true"})
    assert_rows(SparkReader(spark).read(src), [{"id": "1", "v": "a"}, {"id": "2", "v": "b"}])


def test_reads_parquet(spark, tmp_path):
    spark.createDataFrame([(1, "a")], "id int, v string").write.parquet(str(tmp_path / "p"))
    src = Source("files", format="parquet", path=str(tmp_path / "p"))
    assert_rows(SparkReader(spark).read(src), [{"id": 1, "v": "a"}])


def test_read_table(spark):
    spark.createDataFrame([(1,)], "id int").write.saveAsTable("reader_t")
    assert_rows(SparkReader(spark).read_table(TableRef("default", "reader_t")), [{"id": 1}])


def test_table_source_is_rejected(spark):
    with pytest.raises(ValueError, match="table"):
        SparkReader(spark).read(Source("table", table="other"))
