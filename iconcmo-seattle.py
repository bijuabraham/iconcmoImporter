##################################################
# Developed by Mar Thoma Church of San Francisco #
# Author: Abraham and family                     #
# Date: December 2021                            #
##################################################


import argparse
import requests
import dateutil.parser
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

# dev
AUTH = {
	"Phone": "5550000447",
	"Username": "bijuabra",
	"Password": "JungleB00k"
}

relationship_map = {
	"Head of Family": "Husband",
	"Spouse": "Wife"
}

gender_map = {
	"Son": "Male",
	"Daughter": "Female",
	"Spouse": "Female",
	"Head of Family": "Male",
	"Mother": "Female",
	"Father": "Male",
	"Daughter-in-Law": "Female",
	"Grand Daughter": "Female",
	"Brother": "Male",
	"Son-in-law": "Male",
	"Sister": "Female"
}

ENDPOINT = "https://secure1.iconcmo.com/api/"

# data = {
# 	"Auth": AUTH,
# 	"Request": {
# 		"Module": "membership",
# 		"Section": "members"
# 	}
# }


def flag_multiple_heads(f):
	df = pd.read_csv(f)
	f.seek(0)
	by_address = df.groupby(["Address1", "Address2"])
	for addr_tuple, frame in by_address:
		if frame["FamilyRole"].value_counts()["Head of Family"] != 1:
			return True
	return False


def parse_date_string(date_str):
	try:
		dt = dateutil.parser.parse(date_str)
		if dt > datetime.now():
			dt -= relativedelta(years=100)
		return dt.strftime("%m/%d/%Y")

	except dateutil.parser._parser.ParserError:
		print("BAD DATE: {}".format(date_str))
		return None


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


def write_members(individuals_file, households_file, dry_run, household_cap, debug):
	households_df = pd.read_csv(households_file)
	individuals_df = pd.read_csv(individuals_file)
	df = individuals_df.merge(households_df, on=["Address1", "Address2"], validate="many_to_one")
	by_address = df.groupby(["Address1", "Address2"], dropna=False)

	request_body = {
		"Auth": AUTH,
		"Request": {
			"Module": "groups",
			"Section": "householdindex",
			"Action": "read",
		}
	}
	r = make_request(request_body, False, debug)
	group_map = {}
	num_families = 0
	num_members = 0
	for i in range(len(r.json()["householdindex"])):
		if r.json()["householdindex"][i]["category"] == "Prayer Group":
			for group in r.json()["householdindex"][i]["groups"]:
				group_map[group["name"]] = group["id"]
			break
	else:
		assert False, "No prayer group category"

	members_set = set()
	for addr_tuple, frame in by_address:
		if household_cap and num_families > household_cap:
			break
		print("Num families: {}".format(num_families))
		hof = frame[frame.FamilyRole == "Head of Family"]
		first_name = hof.iloc[0]["FirstName"]
		spouse = frame[frame.FamilyRole == "Spouse"]
		if len(spouse.index) > 1:
			raise Exception("Multiple spouses for {}".format(first_name))
		if not spouse.empty:
			first_name += " & {}".format(spouse.iloc[0]["FirstName"])
		data = {
			"first_name": first_name,
			"last_name": hof.iloc[0]["LastName"],
			"city": hof.iloc[0]["City_y"],
			"state": hof.iloc[0]["State_y"],
			"address_1": str(hof.iloc[0]["Address1"]),
			"country": "United States",
			"status_date": "1/1/2021"
		}
		if pd.notna(hof.iloc[0]["Address2"]):
			data["address_2"] = str(hof.iloc[0]["Address2"])
		if pd.notna(hof.iloc[0]["Zip_y"]):
			data["zip"] = str(hof.iloc[0]["Zip_y"])
		if pd.notna(hof.iloc[0]["Email_x"]):
			data["email"] = hof.iloc[0]["Email_x"]
		if pd.notna(hof.iloc[0]["HomePhone_x"]):
			data["phone"] = hof.iloc[0]["HomePhone_x"]
		else:
			if pd.notna(hof.iloc[0]["CellPhone_x"]):
				data["phone"] = hof.iloc[0]["CellPhone_x"]
		request_body = {
			"Auth": AUTH,
			"Request": {
				"Module": "membership",
				"Section": "households",
				"Action": "create",
				"Data": data
			}
		}
		household_id = 0
		r = make_request(request_body, dry_run, debug)
		if r:
			household_id = r.json()["statistics"]["last_id"]
		print("Creating household: {} {}".format(hof.iloc[0]["FirstName"], hof.iloc[0]["LastName"]))
		num_families += 1
		request_body = {
			"Auth": AUTH,
			"Request": {
				"Module": "contributions",
				"Section": "envelopes",
				"Action": "create",
				"Data": {
					"number": str(hof.iloc[0]["Envelope Number"]),
					"household_id": str(household_id)
				}
			}
		}
		make_request(request_body, dry_run, debug)
		print("Adding envelope #: {}".format(str(hof.iloc[0]["Envelope Number"])))

		request_body = {
			"Auth": AUTH,
			"Request": {
				"Module": "groups",
				"Section": "household",
				"Action": "update",
				"Filters": {
					"id": group_map[hof.iloc[0]["Area.1"]]
				},
				"Data": {
					"id": str(household_id)
				}
			}
		}
		make_request(request_body, dry_run, debug)
		print("Adding to group: {}".format(hof.iloc[0]["Area.1"]))

		for _, row in frame.iterrows():
			key = (row["FirstName"], row["MiddleName"] if pd.notna(row["MiddleName"]) else "", row["LastName"])
			role = row["FamilyRole"]
			data = {
				"household_id": str(household_id),
				"first_name": key[0],
				"last_name": key[2],
				"gender": gender_map.get(row["FamilyRole"], "Other"),
				"relationship": relationship_map.get(role) if relationship_map.get(role) else role,
				"status_date": "1/1/2021"
			}
			if role == "Head of Family":
				data["primary"] = True
			special_dates = []
			if pd.notna(row["BirthDate"]):	 	# also parking kerala address & home parish
				dt = parse_date_string(row["BirthDate"])

				if dt:
					birth_date = {
						"id": "Birth",
						"date": dt
					}
					if pd.notna(row["Kerala Address"]):
						birth_date["extra_1"] = row["Kerala Address"].replace('\n', ',')
					if pd.notna(row["Home Parish"]):
						birth_date["extra_2"] = row["Home Parish"]
					special_dates.append(birth_date)

			if row["FamilyRole"] in ("Head of Family", "Spouse") and pd.notna(row["AnnivDate"]):
				dt = parse_date_string(row["AnnivDate"])
				if dt:
					special_dates.append({
						"id": "Marriage",
						"date": dt
					})
			if special_dates:
				data["special_dates"] = special_dates

			phones = []
			if pd.notna(row["HomePhone_x"]):
				phones.append({
					"id": "Home",
					"phone": row["HomePhone_x"]
				})
			if pd.notna(row["CellPhone_x"]):
				phones.append({
					"id": "Mobile",
					"phone": row["CellPhone_x"]
				})
			if pd.notna(row["WorkPhone_x"]):
				phones.append({
					"id": "Work",
					"phone": row["WorkPhone_x"]
				})
			if phones:
				data["phones"] = phones

			emails = []
			if pd.notna(row["Email_x"]):
				emails.append({
					"id": "Personal Email",
					"email": row["Email_x"]
				})
			if pd.notna(row["WorkEmail_x"]):
				emails.append({
					"id": "Work Email",
					"email": row["WorkEmail_x"]
				})
			if emails:
				data["emails"] = emails

			if key[1]:
				data["middle_name"] = key[1]
			if pd.notna(row["Other Name"]):
				data["preferred_name"] = row["Other Name"]

			request_body = {
				"Auth": AUTH,
				"Request": {
					"Module": "membership",
					"Section": "members",
					"Action": "create",
					"Data": data
				}
			}
			make_request(request_body, dry_run, debug)
			print("Creating member: {} {}".format(row["FirstName"], row["LastName"]))
			num_members += 1
		print()
		# if num_families == 10:
		# 	break
	print("Summary: {} families, {} members".format(num_families, num_members))


def delete_all(dry_run, debug):
	request_body = {
		"Auth": AUTH,
		"Request": {
			"Module": "membership",
			"Section": "householdindex",
		}
	}
	r = make_request(request_body, False, debug)
	idxs = []
	for h in r.json()["householdindex"]:
		idxs.append(h["id"])

	request_body = {
		"Auth": AUTH,
		"Request": {
			"Module": "membership",
			"Section": "households",
			"Action": "delete",
			"Filters": {
				"id": idxs
			}
		},
	}
	print("Deleting: {}".format(idxs))
	make_request(request_body, dry_run, debug)


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--individuals", type=argparse.FileType('r'))
	parser.add_argument("--households", type=argparse.FileType('r'))
	parser.add_argument("--household_cap", type=int, default=0)
	parser.add_argument("--debug", action="store_true", default=False)
	parser.add_argument("--dry_run", action="store_true", default=False)
	parser.add_argument("--delete_all", action="store_true", default=False)
	# endpoint = "https://secure1.iconcmo.com/api/"
	#
	# r = requests.get(endpoint, json=data)
	args = parser.parse_args()

	if args.delete_all:
		delete_all(args.dry_run, args.debug)
	else:
		if flag_multiple_heads(args.individuals):
			raise ValueError("Multiple or missing head(s) of family")
		ret = write_members(args.individuals, args.households, args.dry_run, args.household_cap, args.debug)
