import requests

# 코인개코에서 API 로 비트코인,이더리움,리플 가격을 받아옴
def get_crypto_prices_in_krw(crypto_ids):
    url = 'https://api.coingecko.com/api/v3/simple/price'
    params = {
        'ids': ','.join(crypto_ids),
        'vs_currencies': 'krw'
    }
    response = requests.get(url, params=params)
    return response.json()

crypto_ids = ['bitcoin', 'ethereum', 'ripple']
prices = get_crypto_prices_in_krw(crypto_ids)

for crypto, price_info in prices.items():
    print(f"{crypto.capitalize()} price in KRW: {price_info['krw']}")
