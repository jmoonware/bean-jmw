from importertest import converttest
from beanjmw.importers.fido import fido_csv
from beanjmw.importers.qif import qif_importer

# note lex order
# note same file for Fidelity (multiple accts per file)
t_examples=['fidelity.csv','fidelity.csv','test2.qif']
t_accounts=['Assets:US:Fidelity:FX1111','Assets:US:Fidelity:FZ2222','Assets:US:Fidelity:F1234']
t_acct_nums=['1111','2222','1234']
# number of validation errors in non-processed entries
t_errs=[12,30,16] 
t_imp = [fido_csv,fido_csv,qif_importer]
acct_filter="Fidelity"
output_name='old_fido_test.bc'

def test_FidoConvert():
	converttest(t_examples, t_accounts, t_acct_nums, t_errs, t_imp, acct_filter, output_name)	

