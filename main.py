from collections import deque
from datetime import datetime, timedelta
import json
from json_parser import json_parser
import argparse


if __name__ == "__main__":
    print("START...")

    parser = argparse.ArgumentParser(description='Challenge Translation Metrics Stream')
    parser.add_argument('--input_file', help="Path to the file to parse", type=str, required=True)
    parser.add_argument('--window_size', help="Size of the window in seconds", type=int, required=True)

    args = parser.parse_args()
    
    json_file = args.input_file

    json_file_data = json_parser(json_file) # Store JSON Data from arg File.

    current_mean = 0.0 # Init Mean Default Value.
    current_ts = None # Init Current timestamp Default Value.
    deq = deque() # Init Deque Default Value.
    res = dict() # Init Result Default Value.

    file = open('results.json',"w+")

    for json_data in json_file_data:
        timestamp = datetime.strptime(json_data['timestamp'], '%Y-%m-%d %H:%M:%S.%f')

        if len(deq) == 0:
            current_ts = datetime(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute, 0)
            file.write("%s\r\n" % json.dumps({'date': current_ts.strftime('%Y-%m-%d %H:%M:%S'), 'average_delivery_time': current_mean}))
            current_ts += timedelta(minutes=1)

        while current_ts < timestamp:
            while deq and datetime.strptime(deq[0]['timestamp'], '%Y-%m-%d %H:%M:%S.%f') < current_ts - timedelta(minutes=args.window_size):
                deq_v = deq.popleft()
                if deq:
                    current_mean -= (deq_v['duration'] - current_mean)/len(deq)
                else:
                    current_mean = 0
            file.write("%s\r\n" % json.dumps({'date': current_ts.strftime('%Y-%m-%d %H:%M:%S'), 'average_delivery_time': current_mean}))
            current_ts += timedelta(minutes=1)
        
        deq.append(json_data)
        current_mean += (json_data['duration'] - current_mean)/len(deq)
        
    res['date'] = current_ts.strftime('%Y-%m-%d %H:%M:%S')
    res['average_delivery_time'] = current_mean
        
    file.write(f"{json.dumps(res)}\n")
    file.close()
    print(f"FILE {file.name} CREATED SUCESSFULLY...")
    print("END...")