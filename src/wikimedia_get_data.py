

import requests
import json

init_string = 'data: '
source_url = 'https://stream.wikimedia.org/v2/stream/recentchange'

def avro_producer(source_url):
    s = requests.Session()

    with s.get(source_url, headers=None, stream=True) as resp:
        for line in resp.iter_lines():
            if line:
                decoded_line = line.decode()
                if decoded_line.find(init_string) >= 0:
                    # remove data: to create a valid json
                    decoded_line = decoded_line.replace(init_string, "")
                    # convert to json
                    decoded_json = json.loads(decoded_line)
                    print(decoded_json)

avro_producer(source_url)