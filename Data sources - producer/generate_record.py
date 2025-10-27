import random
import csv
from datetime import datetime


# Params
cities = ["Tanger","Tetouan","Marrakech","Casa","Oujda","ALHoceima","Rabat","Agadir"]
tech = ["2G","3G","4G","5G"]
nombre_records = 10000
pct_sms = 0.20
pct_error_inject = 0.05
csv_file = "phone_numbers.csv"
errors = ["missing_field","bad_tech","bad_duration","duplicate"]


# Charger phone numbers from csv file
with open(csv_file, newline='') as f:
    reader = csv.reader(f)
    next(reader)
    phone_numbers = [row[0] for row in reader]


# function for generate a record
def generate_record():
    city = random.choice(cities)
    timestamp = datetime(2025, random.randint(1,12), random.randint(1,28),
                         random.randint(0,23), random.randint(0,59), random.randint(0,59)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    if random.random() < pct_sms:
        record = {
            "record_type": "sms",
            "timestamp": timestamp,
            "sender_id": random.choice(phone_numbers),
            "receiver_id": random.choice(phone_numbers),
            "cell_id": city,
            "technology": random.choice(tech)
        }
    else:
        record = {
            "record_type": "voice",
            "timestamp": timestamp,
            "caller_id": random.choice(phone_numbers),
            "callee_id": random.choice(phone_numbers),
            "duration_sec": random.randint(30, 3600),
            "cell_id": city,
            "technology": random.choice(tech)
        }
    
    # Injection d'erreurs
    if random.random() < pct_error_inject:
        error = random.choice(errors)
        if error == "missing_field":
            if len(record) > 1:
                key_to_remove = random.choice(list(record.keys()))
                del record[key_to_remove]

        elif error == "bad_tech":
            record["technology"] = "unknownTech"

        elif error == "bad_duration":
            if record.get("record_type") == "voice":
                record["duration_sec"] = -random.randint(1,1800)

        elif error == "duplicate":
            return [record,record.copy()]             
 
    return [record]
