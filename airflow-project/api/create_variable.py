import requests

payload_json = {
    "description": "batata",
    "key": "teste",
    "value": "sttesring"
}

auth = {
    "admin": "admin"
}

response = requests.post(
    url='http://localhost:8080/api/v1/variables',
    json=payload_json,
    auth=('admin', 'admin')
)

response.json()