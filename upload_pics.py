##################################################
# Developed by Mar Thoma Church of San Francisco #
# Author: Abraham and family                     #
# Date: December 2021                            #
##################################################
import argparse
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
		print("REQUEST BODY: {}".format(request_body), flush=True)
	if not dry_run:
		r = requests.get(ENDPOINT, json=request_body)
		if r.status_code != requests.codes.ok:
			raise Exception("Bad request")
		elif r.json().get('number'):
			print(request_body, flush=True)
			if (r.json().get("comments")):
				print("COMMENTS: {}".format(r.json()["comments"]), flush=True)
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
	return id_to_env


def upload_pics(dir, mapping, dry_run, debug):
	for h_id in mapping.keys():
		if h_id == '99':
			print("Skipping household 99", flush=True)
			continue
		env_num = mapping[h_id]
		filename = "{}.jpg".format(env_num)
		path = os.path.join(dir, filename)
		try:
			with open(path, "rb") as img_file:
				img = base64.b64encode(img_file.read()).decode('utf-8')
		except FileNotFoundError:
			print("Missing img: {}".format(path), flush=True)
			continue
		data = {
			"picture": img
		}
		request_body = {
			"Auth": AUTH,
			"Request": {
				"Module": "membership",
				"Section": "households",
				"Action": "update",
				"Data": data,
				"Filters": {
					"id": h_id
				}
			}
		}
		print("Writing img at {} to household {}".format(path, h_id), flush=True)
		r = make_request(request_body, dry_run, debug)


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--photodir", required=True)

	parser.add_argument("--debug", action="store_true", default=False)
	parser.add_argument("--dry_run", action="store_true", default=False)

	args = parser.parse_args()
	hid_to_env = get_envelopes(args.dry_run, args.debug)
	upload_pics(args.photodir, hid_to_env, args.dry_run, args.debug)

