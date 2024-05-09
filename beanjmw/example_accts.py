# this works with the example.sh script
# For your own ledgers, make a file called "accts.py" in the downloads 
# directory and add importers as required
# 

from beanjmw.importers.bc import bc_ledger
from beanjmw.importers.csv import csv_general

# mapping of columns in download file to csv importer columns
slate_csv_map={"Trans. Date":'date',"Post Date":'postdate',"Description":'description',"Amount":'amount',"Category":'category'}

CONFIG = [

 csv_general.Importer("Liabilities:US:Chase:Slate",account_number="1234",csv_map=slate_csv_map),
 bc_ledger.Importer("Assets:US:BofA:Checking",reassign=True),
 bc_ledger.Importer("Liabilities:US:Chase:Slate",reassign=True)

]
