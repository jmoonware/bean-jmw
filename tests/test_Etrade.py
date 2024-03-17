from beanjmw.importers.etrade import etrade_csv
from importertest import converttest

# note lex order
t_examples=['etrade.csv']
t_accounts=['Assets:US:Etrade:E2456']
t_acct_nums=['2456']
# number of validation errors in non-processed entries
t_errs=[22] 
t_imp = [etrade_csv]
acct_filter="Etrade:E2456"
output_name="old_etrade.bc"

def test_ImportCsv():
	converttest(t_examples, t_accounts, t_acct_nums, t_errs, t_imp, acct_filter, output_name)

