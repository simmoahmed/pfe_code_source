from extractINFOSTAB import tabs
from extractINFOSCOL import cols
import json

tabs = tabs()
cols = cols()
tab_dict = {}

for tab in tabs:
    tab['columns'] = []
    for col in cols:
        if tab['Name of Table'] == col['Name of Table']:
            tab['columns'].append(col)
    tab_dict[tab['Name of Table']] = tab

with open('output.json', 'w') as json_file:
    json.dump(tab_dict, json_file, indent=4)


