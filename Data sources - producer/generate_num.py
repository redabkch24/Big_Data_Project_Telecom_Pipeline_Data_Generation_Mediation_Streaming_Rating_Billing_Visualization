import random
import csv

def generate_phone_pool(n=300):
    pool = set()
    prefixes = ["6", "7"]
    while len(pool) < n:
        first_digit = random.choice(prefixes)
        rest = ''.join(str(random.randint(0,9)) for _ in range(8))
        pool.add(f"212{first_digit}{rest}")
    return list(pool)

phone_numbers = generate_phone_pool(300)

with open("phone_numbers.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["PhoneNumber"]) 
    for number in phone_numbers:
        writer.writerow([number])

print("Fichier 'phone numbers' generer avec succes ")
