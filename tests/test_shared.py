
from beanjmw.importers.importer_shared import get_transactions

import beanjmw.importers.importer_shared as impsh

# all possible investment actions, no data
all_rows = [impsh.UniRow(action=a) for a in impsh.investment_actions]

account_currency ={'Income:TestOpen':'USD'}

def test_ImporterShared():
	# this makes sure minimal UniRows don't blow up
	res = get_transactions(all_rows, "Assets:Test", "Test Payee", "USD", account_currency)
	assert len(res) == len(all_rows)+3
