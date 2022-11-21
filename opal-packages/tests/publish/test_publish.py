from unittest.mock import MagicMock, patch
from opal.publish import publish, publish_serializer
import json

import numpy as np
import pandas as pd


@patch("requests.post")
def test_publish_called_successfully(mock_post):
    mock_post.return_value = MagicMock(status_code=201)

    success, status = publish(
        kind_id="testid",
        kind_type="testtype",
        kind_metadata={"foo": "bar"},
        api_url="http://test-be",
        token="atesttoken",
    )

    mock_post.assert_called_once_with(
        "http://test-be/instance",
        data=json.dumps(
            dict(
                kind_metadata={"foo": "bar"},
                kind_type="testtype",
                kind_id="testid",
            )
        ),
        headers={
            "content-type": "application/json",
            "Authorization": "token atesttoken",
        },
    )

    assert success == True
    assert status == 201


@patch("requests.post")
def test_publish_called_unsuccessfully(mock_post):
    mock_post.return_value = MagicMock(status_code=500)

    success, status = publish(
        kind_id="testid",
        kind_type="testtype",
        kind_metadata={"foo": "bar"},
        api_url="http://test-be",
        token="atesttoken",
    )

    mock_post.assert_called_once_with(
        "http://test-be/instance",
        data=json.dumps(
            dict(
                kind_metadata={"foo": "bar"},
                kind_type="testtype",
                kind_id="testid",
            )
        ),
        headers={
            "content-type": "application/json",
            "Authorization": "token atesttoken",
        },
    )

    assert success == False
    assert status == 500


def test_publish_serializer_numpy():
    arr = np.array([[1, 2, 3], [4, 5, 6]])
    expected = [[1, 2, 3], [4, 5, 6]]
    assert publish_serializer(arr) == expected
    json.dumps(publish_serializer(arr))


def test_publish_serializer_pandas():
    df = pd.DataFrame({"num": [1, 2, 3], "alph": ["x", "y", None]})
    # expected = {'alph': {0: 'x', 1: 'y', 2: 'z'}, 'num': {0: 1, 1: 2, 2: 3}}
    # assert publish_serializer(df) == expected
    # really this is all we need to test
    json.dumps(publish_serializer(df))
