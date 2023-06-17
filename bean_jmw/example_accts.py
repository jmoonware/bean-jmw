# Example only!
# make a file called "accts.py" in the downloads directory where you 
# exectute the module bean_jmw.bci
#

from beancount.ingest.importers import ofx
from importers.qif import qif_importer

CreditCardNumber1="[0-9]+1234"
CreditCardAccount1="Liabilities:US:CreditCard1"

CheckingNumber="[0-9]+2345"
CheckingAccount="Assets:US:SomeBank:Checking"

CONFIG = [

    ofx.Importer(CreditCardNumber1,
                 CreditCardAccount1),

    qif_importer.Importer(CheckingAccount),
]
