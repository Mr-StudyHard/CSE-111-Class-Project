import sys, json
from app import create_app

app = create_app()
client = app.test_client()

resp_ok = client.post('/api/login', json={'email':'Admin@Test.com','password':'Admin'})
resp_bad = client.post('/api/login', json={'email':'Admin@Test.com','password':'Wrong'})

print('SUCCESS status:', resp_ok.status_code)
print('SUCCESS body:', resp_ok.json)
print('FAIL status:', resp_bad.status_code)
print('FAIL body:', resp_bad.json)

assert resp_ok.status_code == 200 and resp_ok.json.get('ok') is True, 'Expected successful login'
assert resp_bad.status_code == 401 and resp_bad.json.get('ok') is False, 'Expected failed login'
print('Login endpoint tests passed.')
