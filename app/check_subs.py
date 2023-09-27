from __future__ import print_function
from datetime import datetime
import random
import csv
import sys
import json
import hashlib, time
import requests
import urllib3
import multiprocessing.dummy
import os
from contextlib import closing

remaining_data = []
processed_data = []
input_filepath = sys.argv[1]
output_filepath = sys.argv[2]
credential_filepath = sys.argv[3]

urllib3.disable_warnings()

if not os.path.exists(output_filepath):
    with open(output_filepath, "w") as file:
        pass

if os.path.getsize(output_filepath) > 0:
    with open(output_filepath, "r") as fi:
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
            service_id_dict = {"service_id": row["service_id"]}
            processed_data.append(service_id_dict)

with open(input_filepath, "r") as fi:
    reader = csv.DictReader(fi, fieldnames=["service_id"])
    for row in reader:
        search_dict = {"service_id": row["service_id"]}
        if search_dict not in processed_data:
            now = datetime.now()
            date_time = now.strftime("%y%m%d%H%M%S%f")
            transaction_id = (
                "x6"
                + date_time[:15]
                + row["service_id"][-5:]
                + str(random.randint(0, 9))
            )
            container = {
                "transaction_id": transaction_id,
                "channel": "x6",
                "service_id": row["service_id"],
                "current": "false",
            }
            remaining_data.append(container)


def credential(credential_file):
    with open(credential_file, "r") as f:
        credential_json = json.load(f)
    return credential_json


def check_service_id(service_id):
    with open(output_filepath, "r") as file:
        content = file.read()
        if service_id in content:
            return "processed"
        else:
            return "unprocessed"


def request_get(data):
    global response
    currDate = int(time.time())
    credential_api = credential(credential_filepath)
    str2hash = (
        credential_api["api_check_vas_subscription"]["api_key"]
        + credential_api["api_check_vas_subscription"]["secret_key"]
        + str(currDate)
    )
    result = hashlib.md5(str2hash.encode())
    headers = {
        "content-type": str(
            credential_api["api_check_vas_subscription"]["content-type"]
        ),
        "x-signature": result.hexdigest(),
    }
    try:
        data["api_key"] = credential_api["api_check_vas_subscription"]["api_key"]
        response = sess.get(
            url=credential_api["api_check_vas_subscription"]["url_api"],
            params=data,
            headers=headers,
            verify=False,
        )
        print(response.url)
        print(response.headers)
        print(response.status_code)
        print(response.content)
        print("\n")
        respone_content = str(response.content)
        response.raise_for_status()
        if "subscription" in respone_content:
            resp_json = response.json()
            lines_to_append = []
            for subscription in resp_json["subscription"]:
                subscription_id = subscription["id"]
                line = "|".join(
                    [
                        data["transaction_id"],
                        data["channel"],
                        data["service_id"],
                        subscription_id,
                        str(response.status_code),
                        "Success",
                    ]
                )
                lines_to_append.append(line)
            return "\n".join(lines_to_append)
        elif (
            "0|null:null:null|null:null:null|null:null:null|null:null:null|null:null:null"
            in respone_content
        ):
            return "|".join(
                [
                    data["transaction_id"],
                    data["channel"],
                    data["service_id"],
                    "",
                    str(response.status_code),
                    "Has no subscription",
                ]
            )
        elif "System Error" in respone_content:
            return "|".join(
                [
                    data["transaction_id"],
                    data["channel"],
                    data["service_id"],
                    "",
                    str(response.status_code),
                    "System Error",
                ]
            )
    except requests.HTTPError as e:
        if str(e.response.status_code) == "503":
            return "|".join(
                [
                    data["transaction_id"],
                    data["channel"],
                    data["service_id"],
                    "",
                    str(response.status_code),
                    "Service provider unreachable",
                ]
            )
        elif str(e.response.status_code) == "500":
            return "|".join(
                [
                    data["transaction_id"],
                    data["channel"],
                    data["service_id"],
                    "",
                    str(response.status_code),
                    "ESB internal error",
                ]
            )
        elif str(e.response.status_code) == "400" or str(
            e.response.status_code
        ).startswith("502"):
            resp_content = json.loads(str(response.content))
            return "|".join(
                [
                    data["transaction_id"],
                    data["channel"],
                    data["service_id"],
                    "",
                    str(response.status_code),
                    resp_content["message"],
                ]
            )


def main():
    start = time.time()
    batch_size = 7
    with open(output_filepath, "a") as outf:
        with closing(multiprocessing.dummy.Pool(6)) as pool:
            for i in range(0, len(remaining_data), batch_size):
                batch_data = remaining_data[i : i + batch_size]
                batch_results = pool.map(request_get, batch_data)
                for result in batch_results:
                    print(result, file=outf)
                if i + batch_size < len(remaining_data):
                    time.sleep(1)

    elapsed = time.time() - start
    print("\n", "time elapsed is :", elapsed)
    with open(output_filepath + ".complete", "a") as outf:
        print(output_filepath, "completed", file=outf)


if __name__ == "__main__":
    sess = requests.Session()
    main()
