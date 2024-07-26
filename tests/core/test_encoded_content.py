import typing as t

import itertools

import pytest

from saturn_engine.core.encoded_content import ContentEncoding
from saturn_engine.core.encoded_content import ContentEncodings
from saturn_engine.core.encoded_content import ContentType
from saturn_engine.core.encoded_content import ContentTypes
from saturn_engine.core.encoded_content import EncodedContent


@pytest.mark.parametrize(
    "content_type,content_encoding",
    list(
        itertools.product(
            ContentTypes.all, [None] + [[x] for x in ContentEncodings.all]
        )
    ),
)
def test_encode_decode(
    content_type: ContentType,
    content_encoding: t.Optional[list[ContentEncoding]],
) -> None:
    obj = {"x": "0" * 1000}
    encoded = EncodedContent.encode(
        obj, content_type=content_type, content_encoding=content_encoding
    )
    assert isinstance(encoded.content, bytes)
    if content_encoding:
        assert len(encoded.content) < 1000

    decoded = encoded.decode()
    assert decoded == obj
