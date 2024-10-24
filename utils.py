import random
import requests
import logging

def get_proxies():
    with open('proxies.txt', 'r') as f:
        return [line.strip() for line in f if line.strip()]

def get_random_proxy(proxies=None):
    if proxies is None:
        proxies = get_working_proxies()
    return random.choice(proxies) if proxies else None

def test_proxy(proxy):
    try:
        response = requests.get("https://www.google.com", proxies={"http": proxy, "https": proxy}, timeout=5)
        return response.status_code == 200
    except:
        return False

def get_working_proxies():
    all_proxies = get_proxies()
    working_proxies = [proxy for proxy in all_proxies if test_proxy(proxy)]
    logging.info("Found {} working proxies out of {}".format(len(working_proxies), len(all_proxies)))
    return working_proxies
