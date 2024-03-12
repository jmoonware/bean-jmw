from beanjmw.importers.ofx import ofx_general
from importertest import converttest

t_examples=['test1.qfx']
t_accounts=['Assets:US:BofA:Checking']
t_acct_nums=['6789']
t_errs=[32]
t_imp = [ofx_general]
acct_filter="BofA:Checking"
output_name = "old_ofx.bc"

def test_Ofx():
	converttest(t_examples, t_accounts, t_acct_nums, t_errs, t_imp, acct_filter, output_name)

