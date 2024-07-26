import typing as t

import functools
import json
import pickle
import zlib
from dataclasses import dataclass


class ContentType(t.NamedTuple):
    mimetype: str
    decoder: t.Callable[[bytes], object]
    encoder: t.Callable[[object], bytes]


def json_dumps(obj: object) -> bytes:
    return json.dumps(obj).encode()


def json_loads(data: bytes) -> object:
    return json.loads(data.decode())


class ContentTypes:
    JSON = ContentType(
        mimetype="application/json",
        encoder=json_dumps,
        decoder=json_loads,
    )

    PYTHON_PICKLE = ContentType(
        mimetype="application/python-pickle",
        encoder=pickle.dumps,
        decoder=pickle.loads,
    )

    all = [JSON, PYTHON_PICKLE]

    by_mimetype = {t.mimetype: t for t in all}


class ContentEncoding(t.NamedTuple):
    mimetype: str
    decoder: t.Callable[[bytes], bytes]
    encoder: t.Callable[[bytes], bytes]


class ContentEncodings:
    GZIP = ContentEncoding(
        mimetype="application/gzip",
        encoder=functools.partial(zlib.compress, wbits=31),
        decoder=functools.partial(zlib.decompress, wbits=31),
    )

    all = [GZIP]

    by_mimetype = {t.mimetype: t for t in all}


@dataclass(frozen=True)
class EncodedContent:
    content: bytes
    content_type: str
    content_encoding: list[str]

    def decode(self) -> object:
        parser = ContentTypes.by_mimetype.get(self.content_type)
        if not parser:
            raise ValueError(f"Unknown content type: {self.content_type}")

        decoders = []
        for encoding in self.content_encoding:
            typ = ContentEncodings.by_mimetype.get(encoding)
            if not typ:
                raise ValueError(f"Unknown content encoding: {encoding}")
            decoders.append(typ.decoder)

        data = self.content
        for decoder in decoders:
            data = decoder(data)
        return parser.decoder(data)

    @classmethod
    def encode(
        cls,
        obj: object,
        *,
        content_type: ContentType,
        content_encoding: t.Optional[list[ContentEncoding]],
    ) -> "EncodedContent":
        content_encoding = content_encoding or []
        data = content_type.encoder(obj)
        for encoder in content_encoding:
            data = encoder.encoder(data)

        return cls(
            content=data,
            content_type=content_type.mimetype,
            content_encoding=[t.mimetype for t in content_encoding],
        )
