from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from parsel import Selector, SelectorList


class UnsupportedTypeError(Exception):
    pass


SelectorOrCallback = Tuple[str, Optional[Callable[[SelectorList], Any]]]
Rules = Dict[str, SelectorOrCallback]


def single(selectors: SelectorList) -> Optional[str]:
    return selectors.get()


def many(selectors: SelectorList) -> List[str]:
    return selectors.getall()


def call_selector(rule: str, selector: Union[Selector, SelectorList]):
    if selector.type == "text":
        return selector.re(rule)

    if selector.type == "html":
        return selector.css(rule)

    if selector.type == "xpath":
        return selector.xpath(rule)

    if selector.type == "json":
        return selector.jmespath(rule)

    raise UnsupportedTypeError


def extract(rules: Rules, selector: Union[Selector, SelectorList]) -> None:
    data = {}
    for field_name, extraction_rules in rules.items():
        ext_rule, callback = extraction_rules

        if callback:
            data[field_name] = callback(call_selector(ext_rule, selector))
        else:
            data[field_name] = call_selector(ext_rule, selector).get()

    return data


def from_text(
    data: str,
    base_selector: Optional[str] = None,  # noqa
) -> Union[Selector, SelectorList]:
    selector = Selector(data, type="text")
    return selector


def from_json(
    data: str, base_selector: Optional[str] = None
) -> Union[Selector, SelectorList]:
    selector = Selector(data, type="json")
    if base_selector:
        return selector.jmespath(base_selector)

    return selector


def from_html(
    data: str, base_selector: Optional[str] = None
) -> Union[Selector, SelectorList]:
    selector = Selector(data, type="html")
    if base_selector:
        return selector.css(base_selector)

    return selector


def from_xml(
    data: Dict, base_selector: Optional[str] = None
) -> Union[Selector, SelectorList]:
    selector = Selector(data, type="xml")
    if base_selector:
        return selector.xpath(base_selector)

    return selector
