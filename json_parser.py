import json


def json_parser(file):
    data = []
    json_file = open(file, 'r+')

    for line in json_file.readlines():
        json_opened = json.loads(line)
        data.append(json_opened)
    
    return data
