from beanjmw.importers.vanguard import vanguard_csv
from beanjmw.importers.ofx import ofx_general
from importertest import converttest

# note lex order
t_examples=['vanguard.qfx','vanguard_scraped9999.csv']
t_accounts=['Assets:US:Vanguard:V9999','Assets:US:Vanguard:V9999']
t_acct_nums=['9999','9999']
# number of validation errors in non-processed entries
t_errs=[6,12] 
t_imp = [ofx_general, vanguard_csv]
acct_filter="Vanguard"
output_name="old_vanguard.bc"

def test_ImportCsv():
	converttest(t_examples, t_accounts, t_acct_nums, t_errs, t_imp, acct_filter, output_name)

