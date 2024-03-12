from beanjmw.importers.ofx import ofx_general
from beanjmw.importers.qif import qif_importer
from importertest import converttest


# note lex order
t_examples=['test1.qfx','test1.qif','test3.qif']
t_accounts=['Assets:US:BofA:Checking','Assets:US:BofA:Checking','Assets:US:BofA:Checking']
t_acct_nums=['6789','6789','6789']
# number of validation errors in non-processed entries
t_errs=[32,39,24] 
t_imp = [ofx_general, qif_importer, qif_importer]
acct_filter="BofA:Checking"
output_name='old_bofa_test.bc'

def test_BofAConvert():
	converttest(t_examples, t_accounts, t_acct_nums, t_errs, t_imp, acct_filter, output_name)	

