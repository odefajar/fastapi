from __future__ import print_function
from datetime import datetime
import random
import csv
import sys
import json
import urllib3
import hashlib, time
import requests
import multiprocessing.dummy
from contextlib import closing

list_dict = []
input_filepath = sys.argv[1]
output_filepath = sys.argv[2]
credential_filepath = sys.argv[3]

with open(input_filepath, "r") as fi:
    reader = csv.DictReader(
        fi,
        delimiter="|",
        fieldnames=[
            "transaction_id",
            "channel",
            "service_id",
            "subscription_id",
            "status_code",
            "status_desc",
        ],
    )
    for row in reader:
        now = datetime.now()
        date_time = now.strftime("%y%m%d%H%M%S%f")
        transaction_id = (
            "x6" + date_time[:15] + row["service_id"][-5:] + str(random.randint(0, 9))
        )
        if len(str(row["subscription_id"])) != 0:
            container = {
                "transaction_id": transaction_id,
                "channel": row["channel"],
                "service_id": row["service_id"],
                "subscription_id": row["subscription_id"],
            }
            list_dict.append(container)


def credential(credential_file):
    with open(credential_file, "r") as f:
        credential_json = json.load(f)
    return credential_json


def check_service_id(service_id, subscription_id):
    with open(output_filepath, "r") as file:
        content = file.read()
        check_row = service_id + "|" + subscription_id
        if check_row in content:
            return "processed"
        else:
            return "unprocessed"


def request_get(data):
    global response
    currDate = int(time.time())
    credential_api = credential(credential_filepath)
    str2hash = (
        credential_api["api_remove_vas_subscription"]["api_key"]
        + credential_api["api_remove_vas_subscription"]["secret_key"]
        + str(currDate)
    )
    result = hashlib.md5(str2hash.encode())
    headers = {
        "content-type": credential_api["api_remove_vas_subscription"]["content-type"],
        "x-signature": result.hexdigest(),
    }
    try:
        data["api_key"] = credential_api["api_remove_vas_subscription"]["api_key"]
        response = sess.delete(
            url=credential_api["api_remove_vas_subscription"]["url_api"],
            params=data,
            headers=headers,
            verify=False,
        )
        print(response.url)
        print(headers)
        print(response.status_code)
        print("\n")
        response.raise_for_status()
        resp_json = response.json()
        return resp_json
    except requests.HTTPError as e:
        resp_content = json.loads(str(response.content))
        return resp_content


def main():
    start = time.time()
    batch_size = 7
    with closing(multiprocessing.dummy.Pool(6)) as pool:
        with open(output_filepath, "a") as outf:
            for i in list_dict:
                status_service_id = check_service_id(
                    i["service_id"], i["subscription_id"]
                )
                if status_service_id == "unprocessed":
                    resp = request_get(i)
                    transaction_id = resp["transaction"]["transaction_id"]
                    channel = i["channel"]
                    service_id = i["service_id"]
                    status_code = resp["transaction"]["status_code"]
                    status_desc = resp["transaction"]["status_desc"]
                    subscription_id = i["subscription_id"]
                    print(
                        transaction_id,
                        channel,
                        service_id,
                        subscription_id,
                        status_code,
                        status_desc,
                        sep="|",
                        file=outf,
                    )
                time.sleep(1)

    elapsed = time.time() - start
    print("\n", "time elapsed is :", elapsed)
    with open(output_filepath + ".complete", "a") as outf:
        print(output_filepath, "completed", file=outf)


if __name__ == "__main__":
    sess = requests.Session()
    main()
