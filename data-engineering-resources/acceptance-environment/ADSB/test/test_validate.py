import pytest
import pytest_mock
import adsb.validate as validate


@pytest.fixture
def mock_validation_funcs(mocker):
    mocker.patch("adsb.validate._is_bitfield", return_value="bitfield")
    mocker.patch("adsb.validate._convert_timestamp", return_value="epoch")


class TestHiresTraceMetadata:
    def test_is_bitfield_string(self):
        testval = "12.0"
        assert validate._is_bitfield(testval) == False

        testval = "12.4"
        assert validate._is_bitfield(testval) == False

        testval = "12"
        assert validate._is_bitfield(testval) == True

        testval = 12
        assert validate._is_bitfield(testval) == True

        testval = 12.0
        assert validate._is_bitfield(testval) == False

        testval = 12.4
        assert validate._is_bitfield(testval) == False

    def test_convert_timestamp(self):
        epoch_sec = 1651368877.659
        ts = validate._convert_timestamp(epoch_sec)
        assert ts.year == 2022
        assert ts.month == 5
        assert ts.day == 1
        assert ts.hour == 1
        assert ts.minute == 34
        assert ts.second == 37
        assert ts.microsecond == 659000

    def test_validate_field_by_type(self, mock_validation_funcs):
        assert validate._validate_field_by_type(23, "bitfield") == "bitfield"
        assert validate._validate_field_by_type("blah", "epoch") == "epoch"
