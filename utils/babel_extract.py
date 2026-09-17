from typing import Iterable, Mapping, Sequence, Tuple

from jinja2 import Environment
from jinja2.ext import InternationalizationExtension


def extract_jinja2(
    fileobj, keywords: Mapping[str, object], comment_tags: Sequence[str], options=None
) -> Iterable[Tuple[int, str, object, list]]:
    source = fileobj.read()
    if isinstance(source, bytes):
        source = source.decode("utf-8")

    environment = Environment(extensions=[InternationalizationExtension])
    for lineno, funcname, message in environment.extract_translations(
        source, gettext_functions=tuple(keywords)
    ):
        yield lineno, funcname, message, []