from time import sleep

import requests


def test_readiness(im, base_url):
    sleep(6)
    resp = requests.get(base_url + "/readiness")
    assert resp.status_code == 200
