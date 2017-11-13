import json

from jsonschema import ValidationError, validate

from bs4 import BeautifulSoup

schema = {
    "type": "object",
    "required": ["@type"],
    "properties": {
        "@type": {"type": "string"},
        }
    }

microformat = {
    'employee': {
        "type": "object",
        "required": ["url", "name"],
        "properties": {
            "name": {"type": "string"},
            "url": {"type": "string", "format": "uri"},
            "phone": {"type": "string", "format": "phone"},
            "email": {"type": "string", "format": "email"},
            "office": {"type": "string"},
            "office_hours": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "day": {"type": "string"},
                        "start": {"type": "string"},
                        "end": {"type": "string"},
                        "other": {"type": "string"}
                    }
                },
                "minItems": 1,
            }
            },
        }
        }

# test_html = """
# <script type="application/ld+json" id="upolsearch">
# {
# 	"@type": "employee",
# 	"name": "trnecka",
# 	"url": "http://lol.com",
# 	"phone": "+420 585 634 731",
# 	"office": "555123",
# 	"office_hours": [{
# 		"day": "Monday",
# 		"start": "15:40",
# 		"end": "12:22"
# 	}],
# 	"email": "lol@lol.com"
# }
# </script>
# """
#
# soup = BeautifulSoup(test_html, 'html5lib')


def find_microformat_on_page(soup):
    tag = soup.find('script', {'id': 'upolsearch', 'type': 'application/ld+json'})

    json_microformat = tag.next

    return json_microformat


def parse_json(json_microformat):
    try:
        parsed_json = json.loads(json_microformat)
    except Exception as e:
        return None

    return parsed_json


def validate_json_schema(parsed_json):
    try:
        validate(parsed_json, schema)
    except ValidationError as e:
        return False

    data_type = parsed_json.get("@type")

    try:
        validate(parsed_json, microformat.get(data_type))
    except ValidationError as e:
        return False

    return True

# parse_json(find_microformat_on_page(soup))
#
# validate_json_schema(parse_json(find_microformat_on_page(soup)))
