import pytest
import adsb.hires_trace_metadata as md
import adsb.validate as validate


@pytest.fixture
def good_metadata():
    return {
        "icao": "0401a5",
        "r": "ET-AYL",
        "t": "B738",
        "desc": "none today",
        "dbFlags": 1,
        "timestamp": 1651368877.659,
    }


@pytest.fixture
def bad_metadata_missing_field():
    return {
        "icao": "0401a5",
        "t": "B738",
        "desc": "none today",
        "dbFlags": 1,
        "timestamp": 1651368877.659,
    }


class TestHiresTraceMetadata:
    def test_has_valid_fields(self, good_metadata):
        assert md._has_valid_fields(good_metadata) == True

    def test_missing_fields(self, bad_metadata_missing_field):
        assert md._has_valid_fields(bad_metadata_missing_field) == False

    def test_get_metadata_valid(self, good_metadata):

        expected = {}
        raw_metadata = good_metadata
        for field in md.field_data.keys():
            expected[field] = raw_metadata[field]
        ts = validate._convert_timestamp(raw_metadata["timestamp"])
        expected["timestamp"] = ts

        result = md.get_metadata(raw_metadata)
        assert len(result) == 6

        expected_fields = list(expected.keys())
        result_fields = list(result.keys())
        for field in md.field_data.keys():
            assert field in expected_fields
            assert field in result_fields
            assert expected[field] == result[field]
