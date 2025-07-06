from importertest import converttest
from beanjmw.importers.fido import fido_csv, fido_positions
from beanjmw.importers.qif import qif_importer

# note lex order
# note same file for Fidelity (multiple accts per file)
t_examples=['fidelity.csv','fidelity.csv','test2.qif','fido_pos.csv']
t_accounts=['Assets:US:Fidelity:FX1111','Assets:US:Fidelity:FZ2222','Assets:US:Fidelity:F1234','Assets:Fidelity:FZ2222']
t_acct_nums=['1111','2222','1234','2222']
# number of validation errors in non-processed entries
t_errs=[11,31,9,3] 
t_imp = [fido_csv,fido_csv,qif_importer,fido_positions]
acct_filter="Fidelity"
output_name='old_fido_test.bc'
versions=[1,1,None,None]

def test_FidoConvert():
	converttest(t_examples, t_accounts, t_acct_nums, t_errs, t_imp, acct_filter, output_name,versions)

