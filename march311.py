import pandas as pd
import datetime
from dateutil import parser
from collections import Counter


print("Loading in csvs...")
march_with_lat = pd.read_csv("march_raids_with_lat.csv")
march_with_lat['fl_bin'] = march_with_lat.bin_number.astype("float")
binned311 = pd.read_csv("311_Service_Requests_from_2010_to_Present.csv")

number_calls = [] # number of 311 calls on the building targeted by each raid.
within_month = [] # number of 311 calls on the building targeted by each raid in the preceding 30 days.
within_year = [] # number of 311 calls on the building targeted by each raid in the preceding 365 days.
preceding_set = []

MONTH = datetime.timedelta(days = 30)
YEAR = datetime.timedelta(days = 365)

i = 0

old = None

for index, raid in march_with_lat.iterrows():
    if i % 1000 == 0:
        print("Current index: "+str(i))
        print("number_calls (setted): "+str(set(number_calls)))
        print("number_calls (list): "+str(number_calls))
        print("within_month (setted): "+str(set(within_month)))
        print("within_year (setted): " + str(set(within_year)))
        print("preceding_set (setted): "+str(set(preceding_set)))
    raid_time = parser.parse(raid.inspection_date)
    linked_311s = binned311[(binned311['Incident Address'] == raid.address) &
                            (binned311['Borough'] == raid.borough_name.upper())]
    number_calls.append(linked_311s.shape[0])
    try:
        linked_311s['preceding_month'] = linked_311s['Created Date'].map(
            lambda created: True if parser.parse(created) < raid_time and raid_time - parser.parse(created) < MONTH else False
        )
        linked_311s['preceding_year'] = linked_311s['Created Date'].map(
            lambda created: True if parser.parse(created) < raid_time and raid_time - parser.parse(created) < YEAR else False
        )
        preceding_month = linked_311s[linked_311s["preceding_month"] == True]
        preceding_year = linked_311s[linked_311s["preceding_year"] == True]
        within_month.append(preceding_month.shape[0])
        within_year.append(preceding_year.shape[0])
        preceding_set.extend(preceding_year['Complaint Type'].values)
    except AttributeError: # query returned a series
        created = parser.parse(linked_311s['Created Date'])
        if created < raid_time and raid_time - created < MONTH:
            within_month.append(1)
        if created < raid_time and raid_time - created < YEAR:
            within_year.append(1)
            preceding_set.append(linked_311s['Complaint Type'])
    i += 1


march_with_lat['number_calls'] = number_calls
march_with_lat['preceding_month'] = within_month
march_with_lat.to_csv("march_with_raid_links.csv",index=False)
print("proportion ever 311'd: ", str(float(len([v for v in number_calls if v > 0])) / march_with_lat.shape[0]))
print("proportion preceded by 311 (30 days): "+str(float(len([v for v in within_month if v > 0])) / march_with_lat.shape[0]))
print("proportion preceded by 311 (365 days): "+str(float(len([v for v in within_year if v > 0])) / march_with_lat.shape[0]))

preceding_set = Counter(preceding_set)
print(preceding_set)
pd.DataFrame.from_records(preceding_set).to_csv("preceding.csv", index=False)