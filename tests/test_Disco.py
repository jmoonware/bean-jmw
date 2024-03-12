from beanjmw.bci import process_extracted_entries, filter_entries
import beanjmw.bci as bci
from beanjmw.importers.csv import csv_general
from beanjmw.importers.qif import qif_importer
from importertest import converttest

# note lex order
t_examples=['discover6789.csv','discover6789.qif']
t_accounts=['Liabilities:US:Discover:D6789','Liabilities:US:Discover:D6789']
t_acct_nums=['6789','6789']
# number of validation errors in non-processed entries
t_errs=[29, 44] 
t_imp = [csv_general, qif_importer]
acct_filter="Discover"
output_name="old_discover.bc"

def test_ImportCsv():
	converttest(t_examples, t_accounts, t_acct_nums, t_errs, t_imp, acct_filter, output_name)
	

