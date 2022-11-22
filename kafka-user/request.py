import requests
import json
parameters={
    "id": 20344,
    "nickname": "bastelfreak",
}
response = requests.get("https://24pullrequests.com/users.json?id={id}&nickname={nickname}",params=parameters)
print(response.status_code)

# dumping the response into json file with beautification
with open('response.json', 'w') as file:
    json.dump(response.json(), file, indent=4)
    