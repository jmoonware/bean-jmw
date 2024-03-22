![build workflow](https://github.com/jmoonware/bean-jmw/actions/workflows/python-app.yml/badge.svg)
# bean-jmw
Code for using beancount v2 plain-text accounting package.

This project implements per-account automatic payee and account assignment to transactions (stored in yaml config files), and deduplication. Here is a brief description of each script:

* bci.py - identify, extract, or file actions for updating ledger
* bcr.py - reporting script, makes static text or HTML reports
* bar.py - dissects portfolio by sector, individual stock, and other categories

In addition there are some utility scripts 

* plot_things.py - makes plots of values (by time)
* plot_networth.py - specialized script to plot net worth over time
* yaml_util.py - organizes, sorts, possibly simplifies yaml assignment rules

To see examples how each script can be used, run the example.sh bash script. 

Here are the setup and usage instructions:

Create a financial root directory somewhere (say FINROOT=/home/myusername/Finance) 

Clone this project (bean-jmw) inside this financial root directory, although the repository can be anywhere.

i.e.

```
mkdir $FINROOT
cd $FINROOT
git clone https://github.com/jmoonware/bean-jmw
```

Create a venv using python 3.9, because beancount needs this, activate this virtual environment, and install beanjmw via

```
python3.9 -m venv avenv
source avenv/bin/activate
cd bean-jmw
pip install -e . 
```

Create a 'private' sub-directory in $FINROOT - this is where all your personal info goes (i.e. downloaded transactions, ledgers, account numbers, etc.)

Inside $FINROOT/private, create a "downloads" directory. Download stuff from your financial institutions here. I also create a parallel "files" directory that contains all previously ingested files.

You need to create an acct.py file in the 'private/downloads' directory. An example of the contents can be found in example_accts.py in the bean-jmw repository. Copy the example_accts.py file to the downloads directory, rename it accts.py, and modify the contents appropriately. The example script example.sh does this as well.

When in the 'downloads' directory, use 

```
python -m beanjmw.bci [identify|extract|file]
```

Use the 'extract' option iteratively: First time through, if a checking account is being ingested, then all the checks with unassigned payees will go to yaml file $FINROOT/private/downloads/yaml/Accountname_payees_unassigned.yaml. Once check payees are assigned, any accounts that can't be assigned from an existing yaml rule in yaml/Accountname.yaml will be put in yaml/Accountname_unassigned.yaml. See the Wiki for detailed instructions.

When using 'file', don't forget the '-o ../files' option, which will put your re-named downloaded financial transaction files in directories parallel to 'downloads'. Since identify and extract look at all subdirectories when ingesting, we need to move these files to another branch of the directory tree.

I work on the ledger in the 'downloads' directory, then when happy I move it up to the 'private' directory and rename it 'ledger.beancount' or something. If you want to keep backup ledgers you can do that manually of course, or use git or some other version control mechanism.

Here is  an example directory structure that will result:

```bash
$FINROOT
├── bean-jmw
│   └── importers
│       └── filters
└── private
    ├── files
    │  	├── Assets
    │   │   └── US
    │   │       └── Bank1
    │   │           ├── Checking
    │   │           └── Savings
    │   └── Liabilities
    │       └── US
    │           └── Card1
    └── downloads
        └── yaml
```

The 'private' folder contains all the stuff that should be kept private. To back up, I create a an encrypted private.tar archive manually.
