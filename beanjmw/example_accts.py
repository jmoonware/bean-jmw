# this works with the example.sh script
# For your own ledgers, make a file called "accts.py" in the downloads 
# directory and add importers as required
# 

from importers.bc import bc_ledger

CONFIG = [
 
 bc_ledger.Importer("Assets:US:BofA:Checking",reassign=True),
 bc_ledger.Importer("Liabilities:US:Chase:Slate",reassign=True)

]
