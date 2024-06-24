from main import parse_config


def test_parse_config():
    parsed_config = parse_config("src/test/resources/test_yaml.yaml")
    expected = "test_value2"
    actual = parsed_config.get("test1").get("test2")
    assert expected == actual
