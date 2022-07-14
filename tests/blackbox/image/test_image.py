import requests


def test_readiness(container, base_url):
    resp = requests.get(base_url + "/readiness")
    assert resp.status_code == 200
