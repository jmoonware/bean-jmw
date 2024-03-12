from importers.qif import qif_importer
from importers.fido import fido_csv
from importers.ofx import ofx_general
from importers.csv import csv_general
from importers.bc import bc_ledger
from importers.etrade import etrade_csv
from importers.vanguard import vanguard_csv

# mapping of columns in download file to csv importer columns
discover_csv_map={"Trans. Date":'date',"Post Date":'postdate',"Description":'description',"Amount":'amount',"Category":'category'}

CONFIG = [

csv_general.Importer("Liabilities:US:Discover:D6789",account_number="6789",csv_map=discover_csv_map),
ofx_general.Importer("Assets:US:BofA:Checking", account_number="[0-9]+6789"),
ofx_general.Importer("Assets:US:Vanguard:V9999", account_number="[0-9]+9999"),
fido_csv.Importer("Assets:US:Fidelity:FX1111",account_number='1111'),
fido_csv.Importer("Assets:US:Fidelity:FZ2222",account_number='2222'),
qif_importer.Importer("Assets:US:Fidelity:F1234"),
qif_importer.Importer("Assets:US:BofA:Checking"),
qif_importer.Importer("Liabilities:US:Discover:D6789"),
etrade_csv.Importer("Assets:US:Etrade:E6789"),
vanguard_csv.Importer("Assets:US:Vanguard:V9999",account_number='99999999'),

]
