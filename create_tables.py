import sqlite3

conn = sqlite3.connect('identifier.sqlite')
print("Connected to database successfully")

conn.execute('CREATE TABLE earnings (year TEXT, month TEXT, total_sales FLOAT, in_eu FLOAT, outside_eu FLOAT, by_card FLOAT, by_paypal FLOAT, siik_payout FLOAT, sickrate_payout FLOAT)')
print("Created table successfully!")

conn.close()