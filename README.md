![build workflow](https://github.com/jmoonware/bean-jmw/actions/workflows/python-app.yml/badge.svg)
# bean-jmw
Code for using beancount v2 plain-text accounting package. 

This project implements per-account automatic payee and account assignment to transactions (stored in yaml files), and deduplication. Here is a brief description of each script (see the [Wiki](https://github.com/jmoonware/bean-jmw/wiki) for more details):

* stage.py - Used to iteratively import new transactions into per-account ledgers - uses:
  * bci.py - identify, extract, or file actions for updating ledger

The following reporting scripts are available:
* bcr.py - reporting script, makes static text or HTML reports
* bar.py - dissects portfolio by sector, individual stock, and other categories, scraping info from the web as needed

In addition there are some utility scripts:

* plot_things.py - makes plots of account balances or changes, optionally with cost basis (by time)
* recent_net_worth.py - prints/plots net worth total (and by institutions)
* yaml_util.py - organizes, sorts, possibly simplifies yaml assignment rules
* Less important/deprecated:
  * plot_networth.py (deprecated) - simple specialized script to plot net worth over time
  * bal.py - plots (and prints) the difference between balance of specific account and supplied balance values in a text file (useful for tracking down where balances diverge)

To see examples how each script can be used, run/read the example.sh bash script. 


## Setup instructions:

Create a financial root directory somewhere (say FINROOT=/home/myusername/Finance.) The $FINROOT directory is where all your personal info goes (i.e. downloaded transactions, ledgers, account numbers, etc.)

If you don't have python 3.9, then download and install this version first. Create a venv using python 3.9, activate this virtual environment, and install beanjmw via.

```
python3.9 -m venv avenv
source avenv/bin/activate
```

Clone this project (bean-jmw) somewhere. This can be in the $FINROOT directory, although the repository can be anywhere. My preference is to keep it 'parallel' to $FINROOT. For example, if $FINROOT is the directory 'finroot'

```
mkdir finroot
git clone https://github.com/jmoonware/bean-jmw
cd bean-jmw
pip install -e . 
```

I also create an alias:

```
alias stage='python -m beanjmw.stage'
```

Inside $FINROOT, create a "downloads" directory. Download stuff from your financial institutions here. I also create a parallel "files" directory that contains all previously ingested files. The 'stage.py' script will also create a couple other directories (backup, staging) when you run it for the first time.

You need to create an accts.py file in the 'downloads' directory. An example of the contents can be found in example_accts.py in the bean-jmw repository, which is also used in the examples.sh script. Copy the example_accts.py file to the downloads directory, rename it accts.py, and modify the contents appropriately. The accts.py file contains the CONFIG list of importers used by beancount, and optionally a dict of ledger name prefixes by accounts for keeping separate ledgers.

## Basic Usage Instructions

Typically, the steps of updating the ledgers consists of the following:

* If ledgers already exist, run 'stage --last' to see when each ledger was last updated. Use this info to download the right range of dates in the next step.
* Download new transactions into the _downloads_ directory. This is done manually through each institution's web interface at the moment.
* Identify the files
  * Run 'stage --identify' to verify that the downloaded files will be imported with the expected importers defined in accts.py (if not, accts.py and/or the downloaded files may require some hand-editing.)
* Run _extract_ to create new transactions in beancount format
  * If using the stage.py script (the ledger dict in accts.py is set-up)
    > stage --extract
    * The stage script will create a ../staging directory with release candidate ledger files.
  * To do this step manually, on each ledger run:
    > python -m beanjmw.bci extract -e _existing-ledger_ > _new-transactions_
    * This will extract the new, downloaded transactions and put them in _new-transactions_. The transactions in _new-transactions_ should be incremental (i.e. deduplicated from the _existing-ledger_.)
* Update yaml rules for any unassigned accounts
  * Typically, on the first usage of 'extract', a few new _unassigned_.yaml files will be created in the downloads/yaml directory (or appended if they already exist), assigning any new transactions that didn't match a rule to 'UNASSIGNED' accounts.
  * Edit the _Account_.yaml files in the download/yaml directory to change the new UNASSIGNED accounts to the accounts that are desired (usually I append each _Account_ unassigned.yaml file to the _Account_.yaml file as a start, then edit from there.)
* Re-run _extract_ with updated rules
* Create new ledger release candidate
  * The stage.py script does this automatically in the 'staging' directory and will run _bean-check_ to make sure it is correct
  * If you are doing this manually, concatenate _existing-ledger_ with _new-transactions_ to a 'release candidate' ledger file (I usually append 'rc' to the name), then run bean-check on this file. If it passes, great- your ledger is now updated. Back-up your _existing-ledger_, then move the release candidate to the _existing-ledger_. 
* Backup old ledger, move release candidate to current ledger
  * The stage.py script does this with the --update option (and optional --remove option, which cleans up the files) i.e.
    > stage --update --remove
* Archive the downloaded files: 
  * Run 
    > stage --archive 
* Run reports on new ledger(s)
  * The example.sh file has examples of how to generate reports

Here is the directory structure that will result from the example.sh script:

```bash

Example_Test/
├── backup
├── downloads
│   └── yaml
├── files
├── reports
│   └── yaml
└── staging

```

The ledger(s) will be in the Example_Test directory (parallel to the sub-folders backup, etc., above). 

The example.sh script demonstrates the stage.py script, as well as how to use the bci.py script directly.

Right now this project only supports Linux, although I might add Windows support in the future if there is interest.
