import pandas as pd
import datetime
from dateutil import parser
from collections import Counter


print("Loading in csvs...")
march_with_lat = pd.read_csv("march_raids_with_lat.csv")
march_with_lat['fl_bin'] = march_with_lat.bin_number.astype("float")
binned311 = pd.read_csv("311_Service_Requests_from_2010_to_Present.csv")

MONTH = datetime.timedelta(days = 30)
YEAR = datetime.timedelta(days = 365)
INFRACTIONS = set([
    "Noise - Commercial",
    "Noise - Residential",
    "Noise - Street/Sidewalk",
    "Food Establishment",
    "Noise Survey",
    "Drinking",
    "Building/Use",
    "Consumer Complaint",
    "Dirty Conditions",
    "Noise",
    "UNSANITARY CONDITION",
    "Graffiti",
    "Rodent",
    "Smoking",
    "PAINT/PLASTER",
    "PAINT - PLASTER",
    "DOOR/WINDOW",
    "Other Enforcement",
    "GENERAL",
    "Blocked Driveway",
    "FLOORING/STAIRS",
    "Sanitation Condition",
    "Special Projects Inspection Team (SPIT)",
    "Food Poisoning",
    "Electrical",
    "WATER LEAK",
    "Non-Emergency Police Matter",
    "Plumbing",
    "SAFETY",
    "Indoor Air Quality",
    "Non-Residential Heat",
    "Elevator",
    "Animal Abuse",
    "Industrial Waste",
    "Water Quality",
    "Lead",
    "Hazardous Materials",
    "Asbestos",
    "Vending",
    "Mold",
    "Investigations and Discipline (IAD)",
    "Drinking Water",
    "Boilers",
    "Noise - House of Worship",
    "Standing Water",
    "Scaffold Safety",
    "OUTSIDE BUILDING",
    "Root/Sewer/Sidewalk Condition",
    "School Maintenance",
    "Traffic",
    "Noise - Park",
    "Beach/Pool/Sauna Complaint",
    "Drug Activity",
    "Indoor Sewage",
    "Cranes and Derricks",
    "Water System",
    "General Construction/Plumbing",
    "HEATING",
    "PLUMBING",
    "HEAT/HOT WATER"]
)

number_calls = [] # number of 311 calls on the building targeted by each raid.
within_month = [] # number of 311 calls on the building targeted by each raid in the preceding 30 days.
within_month_valid = []
within_year = [] # number of 311 calls on the building targeted by each raid in the preceding 365 days.
within_year_valid = []
preceding = []
preceding_valid = []
preceding_set = []

def zeros():
    preceding.append(0)
    preceding_valid.append(0)
    within_month_valid.append(0)
    within_month.append(0)
    within_year.append(0)
    within_year_valid.append(0)

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
        preceding_311s = linked_311s[linked_311s['Created Date'].map(
            lambda created: True if parser.parse(created) < raid_time else False
        )]
        preceding_311s['relevant_infraction'] = preceding_311s['Complaint Type'].isin(INFRACTIONS)
        preceding_311s['preceding_year'] = linked_311s['Created Date'].map(
            lambda created: True if (raid_time - parser.parse(created)) < YEAR else False
        )
        preceding_311s['preceding_month'] = preceding_311s['Created Date'].map(
            lambda created: True if (raid_time - parser.parse(created)) < MONTH else False
        )
        preceding.append(preceding_311s.shape[0])
        if preceding_311s.shape[0] > 0:
            preceding_valid.append(preceding_311s[(preceding_311s.relevant_infraction.notnull()) & (preceding_311s.relevant_infraction)].shape[0])
            preceding_month = preceding_311s[preceding_311s["preceding_month"]]
            preceding_month_valid = preceding_month[(preceding_month.relevant_infraction.notnull()) & (preceding_month.relevant_infraction)]
            preceding_year = preceding_311s[preceding_311s["preceding_year"]]
            preceding_year_valid = preceding_year[(preceding_year.relevant_infraction.notnull()) & (preceding_year.relevant_infraction)]
            within_month.append(preceding_month.shape[0])
            within_month_valid.append(preceding_month_valid.shape[0])
            within_year.append(preceding_year.shape[0])
            within_year_valid.append(preceding_year_valid.shape[0])
            preceding_set.extend(preceding_year['Complaint Type'].values)
        else:
            zeros()
    except AttributeError: # query returned a series
        created = parser.parse(linked_311s['Created Date'])
        if created < raid_time:
            preceding.append(1)
            is_inf = linked_311s['Complaint Type'] in INFRACTIONS
            if is_inf:
                preceding_valid.append(1)
            else:
                preceding_valid.append(0)
            if (raid_time - created) < YEAR:
                preceding_set.append(linked_311s['Complaint Type'])
                within_year.append(1)
                if is_inf:
                    within_year_valid.append(1)
                else:
                    within_year_valid.append(0)
                if (raid_time - created) < MONTH:
                    within_month.append(1)
                    if is_inf:
                        within_month_valid.append(1)
                    else:
                        within_month_valid.append(0)
        else:
            zeros()
    except KeyError: # no preceding
        print("passing on "+str(i))
        zeros()
    i += 1


march_with_lat['number_calls'] = number_calls
march_with_lat['preceding_month'] = within_month
march_with_lat.to_csv("march_with_raid_links.csv",index=False)
print("proportion ever 311'd: ", str(float(len([v for v in number_calls if v > 0])) / march_with_lat.shape[0]))
print("proportion preceded by 311 (since 2010): "+str(float(len([v for v in preceding if v > 0])) / march_with_lat.shape[0]))
print("proportion preceded by 311 (30 days): "+str(float(len([v for v in within_month if v > 0])) / march_with_lat.shape[0]))
print("proportion preceded by 311 (365 days): "+str(float(len([v for v in within_year if v > 0])) / march_with_lat.shape[0]))
print("valid proportion preceded by 311 (30 days): "+str(float(len([v for v in within_month_valid if v > 0])) / march_with_lat.shape[0]))
print("valid proportion preceded by 311 (365 days): "+str(float(len([v for v in within_year_valid if v > 0])) / march_with_lat.shape[0]))


preceding_set = Counter(preceding_set)
print(preceding_set)
