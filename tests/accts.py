from importers.qif import qif_importer
from importers.fido import fido_csv
from importers.ofx import ofx_general
from importers.csv import csv_general
from importers.bc import bc_ledger

# mapping of columns in download file to csv importer columns
discover_csv_map={"Trans. Date":'date',"Post Date":'postdate',"Description":'description',"Amount":'amount',"Category":'category'}

CONFIG = [

csv_general.Importer("Liabilities:US:Discover:Blarg",account_number="6789",csv_map=discover_csv_map),
ofx_general.Importer("Assets:US:BofA:Checking", account_number="[0-9]+6789"),
fido_csv.Importer("Assets:US:Fidelity:F1234"),
qif_importer.Importer("Assets:US:BofA:Checking"),

]
