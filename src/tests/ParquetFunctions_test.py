import pytest


def test_subfolder_answer():
    # test pf.read_parquet_current_folder
    import SandsPythonFunctions.ParquetFunctions as pf

    subfolder = ["parquet_test_files"]
    dta = pf.read_parquet_current_folder(subfolder=subfolder)
    assert dta.shape == (25, 4)


def test_no_files():
    # test pf.read_parquet_current_folder
    import SandsPythonFunctions.ParquetFunctions as pf

    dta = pf.read_parquet_current_folder()
    assert dta is None


def test_concatenating_dataframes():
    import SandsPythonFunctions.ParquetFunctions as pf

    subfolder = ["parquet_test_files"]
    dta_snappy = pf.read_parquet_current_folder(pattern="SNAPPY", subfolder=subfolder)
    dta_zstd = pf.read_parquet_current_folder(pattern="ZSTD", subfolder=subfolder)
    dataframes = [dta_snappy, dta_zstd]
    dta = pf.concat_dataframes(dataframes)
    assert dta.shape == (50, 4)


print("If there is an error run pytest inside SandsPythonFunctions/src/tests")
