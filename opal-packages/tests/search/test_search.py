from opal.query import Instance, Results
from unittest.mock import patch
import pytest
import json


@pytest.fixture(scope="module")
def instance():
    inst = Instance(api_url="whatever", token="whatever")
    return inst


@patch("requests.get")
def test_request_called_on_search(mock_get, instance):
    instance.search()
    mock_get.assert_called_once_with(
        "whatever/instance", headers={"Authorization": "token whatever"}
    )


@patch("requests.get")
def test_proper_formation_of_query(mock_get, instance):
    instance._with("foo", "bar")._and("boo", "far")._or("bar", "foo")._not(
        "hump", "day"
    ).search()

    should_expect = [
        dict(key="foo", value="bar"),
        dict(key="boo", value="far", operator="AND"),
        dict(key="bar", value="foo", operator="OR"),
        dict(key="hump", value="day", operator="NOT"),
    ]

    assert instance.query == should_expect


@patch("requests.get")
def test_pass_type_argument(mock_get, instance):
    instance._with("a", "b")._type("giant_yogurt").search()

    assert instance.type == "giant_yogurt"


@patch("requests.get")
def test_valid_json2hex(mock_get, instance):
    instance._with("a", "b").search()

    expected_query_group = [dict(key="a", value="b")]
    str_query = json.dumps(expected_query_group)
    hex_query = str_query.encode("utf-8").hex()

    full_query = f"whatever/instance?search={hex_query}"

    mock_get.assert_called_once_with(
        full_query, headers={"Authorization": "token whatever"}
    )


@patch("requests.get")
def test_instance_returns_results_class(mock_get, instance):
    r = instance.search()
    assert isinstance(r, Results)


def test_result_methods():
    rd = dict(
        data=[
            dict(kind_id="test", kind_type="test", kind_metadata=dict(foo="bar")),
            dict(kind_id="test", kind_type="test", kind_metadata=dict(foo="bar")),
            dict(kind_id="test", kind_type="test", kind_metadata=dict(foo="bat")),
        ]
    )

    res = Results(response_dict=rd)

    assert res.all() == rd.get("data")
    assert res.ids() == [x.get("kind_id") for x in rd.get("data")]
    assert len(res.filter(lambda x: x["kind_metadata"]["foo"] == "bar")) == 2
    assert res.reduce(lambda x: x["kind_type"]) == ["test", "test", "test"]
