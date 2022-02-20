##################################################
# Developed by Mar Thoma Church of San Francisco #
# Author: Abraham and family                     #
# Date: December 2021                            #
##################################################
import argparse
import csv
import json
import traceback

import requests
import os
import base64


# dev
AUTH = {
    "Phone": "phonenumber",
    "Username": "username",
    "Password": "password"
}

ENDPOINT = "https://secure1.iconcmo.com/api/"

def make_request(request_body, dry_run, print_request):
    if print_request:
        print("REQUEST BODY: {}".format(request_body))
    if not dry_run:
        r = requests.get(ENDPOINT, json=request_body)
        if r.status_code != requests.codes.ok:
            raise Exception("Bad request")
        elif r.json().get('number'):
            print(request_body)
            if (r.json().get("comments")):
                print("COMMENTS: {}".format(r.json()["comments"]))
            raise Exception(r.json()["message"])
        return r


def get_envelopes(dry_run, debug):
    request_body = {
        "Auth": AUTH,
        "Request": {
            "Module": "contributions",
            "Section": "envelopes",
        }
    }
    r = make_request(request_body, False, debug)
    id_to_env = {}
    for h in r.json()["envelopes"]:
        id_to_env[h["household_id"]] = h["number"]
        if debug:
            print "HH {}".format(json.dumps(h, indent=4))
    return id_to_env

def get_pg(hhid_to_env_dict, dry_run, debug):
    request_body = {
        "Auth": AUTH,
        "Request": {
            "Module": "groups",
            "Section": "household",
        }
    }
    r = make_request(request_body, False, debug)
    full_hh_list = []
    for h in r.json()["household"]:
        if debug:
            print "HH {}".format(json.dumps(h, indent=4))
        pg_list = h.get('groups')
        for pg in pg_list:
            area = pg.get('name')
            hh_in_area_list = pg.get('households')
            for hh in hh_in_area_list:
                record = dict()
                try:
                    env = hhid_to_env_dict.get(hh.get('id'))
                    if env:
                        record['Household Record ID'] = hh.get('id')
                        record['Prayer Group'] = area
                        record['Donor #'] = env
                        full_hh_list.append(record)
                    else:
                        print('Env not found for hhid {}'.format(hh.get('id')))
                except:
                        print('Exception : {}'.format(traceback.format_exc()))
    return full_hh_list


def list_to_csv(out_file, hh_list):
    csv_cols = ["Household Record ID", "Prayer Group", "Donor #"]
    with open(out_file, 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_cols)
        writer.writeheader()
        for record in hh_list:
            writer.writerow(record)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--auth", required=True)
    parser.add_argument("--username", default="")
    parser.add_argument("--cred", default="")
    parser.add_argument("--out", required=True)

    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument("--dry_run", action="store_true", default=False)

    args = parser.parse_args()

    AUTH['Phone'] = args.auth
    AUTH['Username'] = args.username if args.username else ''
    AUTH['Password'] = args.cred if args.cred else ''
    hid_to_env = get_envelopes(args.dry_run, args.debug)
    full_list = get_pg(hid_to_env, args.dry_run, args.debug)
    list_to_csv(args.out, full_list)

