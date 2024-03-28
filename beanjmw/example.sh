#
# Script to run an example ledger through beanjmw scripts
#
# Make sure you install beanjmw and the dependencies first (instructions
# in the readme file)
# Also make sure to activate the appropriate python venv
#
EXAMPLE_DIR=Example_Test
LEDGER=example.txt
REPORT_DATE=`date -Idate`
# aliases for scripts
BCI="python -m beanjmw.bci"
BCR="python -m beanjmw.bcr"
BAR="python -m beanjmw.bar"
PNW="python -m beanjmw.plot_networth"
PT="python -m beanjmw.plot_things"

REPORT_YEARS=7

# year before current (incomplete) year
END_YEAR=$((`date +"%Y"`-1))
START_YEAR=$((END_YEAR-REPORT_YEARS))

# kludge to find where the accts_example.py file is installed
BEANJMW_PATH=`python -c "import beanjmw; print(beanjmw.__path__[0])"`

# create the directory structure 
mkdir $EXAMPLE_DIR
cd $EXAMPLE_DIR
mkdir reports
mkdir downloads

# create an example ledger for a number of years
bean-example --date-begin $START_YEAR-01-01 -o $LEDGER

# Copy example accts.py file to downloads directory
# modify this file for your importers
# Here, we are using the custom "beancount ledger importer" bc_importer
# This importer ingests an existing ledger, which might seem useless at first,
# but it allows us to pass existing ledgers through the auto-assign code
# This gives us a way to create auto-assign rules from existing ledgers
#
# A similar process takes place in the QIF importer if the Quicken
# transactions are already categorized - bci will guess at an account
# based on the category (but since Quicken account names follow different
# rules, the resulting accounts in yaml rules files may require some tlc.)
#
cd downloads
cp $BEANJMW_PATH/example_accts.py accts.py
# make a copy of the example ledger in the download folder
# Normally, this is where we download new transactions
# For the example, we will "pretend" this ledger is the new download
# in order to create the yaml assignment rules
cp ../$LEDGER .

# First pass: this creates the yaml directory
# and auto-assign yaml files
# Each of these files will be called {Account_Name}_unassigned.yaml
# where Account_Name is full account name with ":" replaced by "_"
$BCI extract > ../t.txt

# Now move the unassigned files to the files that will be used 
# here we just copy and remove the _unassigned part of the name
# Once you have a pre-existing account yaml file, each new ingest may
# create an unassigned.yaml file for the account. This may need to 
# be appended to the existing file, then hand-edit
# the assignments

echo "=== Making yaml account assignment files from unassigned..."
python -c 'import glob,shutil; print([shutil.copy(x,x.replace("_unassigned","")) for x in glob.glob("yaml/*.yaml")])'

# Now extract again, but this time using the auto-assign yaml
# files
# This is unexciting, as we created the rules from this exact file,
# nothing changes; in fact, t.txt should be empty of transactions
# since they are all duplicates of the original
# 
$BCI extract -e ../$LEDGER > t.txt

# t.txt should be empty of transactions at this point
# all the transactions in example.txt are duplicates by definition
echo "=== Checking for any non-duplicate transactions..."
python -c 'from beancount.loader import load_file; entries,errs,config=load_file("t.txt"); [print(t) for t in entries]'

# we can also simplify some assignment rules
# yaml_util will look for regex patterns that all map to the same account
# yaml_util just writes to stdout, so if you wanted to use the simplified 
# rules then you would need to copy them into the proper yaml file
echo "=== Simplified rules example going to combined simplified.yaml..."
python -c 'import glob,os; cmds=["python -m beanjmw.yaml_util -e {} -sp >> yaml/simplified.yaml".format(x) for x in glob.glob("yaml/*.yaml") if not "unassigned" in x]; [os.system(c) for c in cmds]'

# now make some reports
cd ../reports

# bcr.py makes top level Income, Expense, Assets, Liabilities reports 
# I like to keep them separate - just my preference
# Note that we create a 'bcr.tsv' file by default, which contains a 
# chart of accounts
# You can exclude any account from a report by changing the first 'y' to 
# 'n' in bcr.tsv. You can also make copy and customize versions of bcr.tsv 
# Use the '--config_file' option in bcr to use the custom file 

for t in 'Expenses' 'Income'
do
echo "=== $t report for 2023 with details..."
 $BCR -f ../$LEDGER -t $t -ma -dt -html -sd 2023-01-01 -ed 2023-12-31  > $t$REPORT_DATE.html
done

# without end date these are just the latest values
for t in 'Assets' 'Liabilities'
do
echo "=== $t report with details..."
 $BCR -f ../$LEDGER -t $t -dt -html  > $t$REPORT_DATE.html
done

# Let's do a net worth progression and save it to file - will plot below
echo "=== Assets progression by year..."
# Reports for above ledger
for t in 'Assets' 'Liabilities'
do
	echo -e "start_date\tend_date\tGrandTotal\tValue" > NW_$t$REPORT_DATE.txt
done

for ((i = $START_YEAR; i <= $END_YEAR; i++))
do
	for t in 'Assets' 'Liabilities'
	do
		echo $i,$t
		$BCR -f ../$LEDGER -sd $i-01-01 -ed $i-12-31 -t $t -pt -np  | grep GrandTotal >> NW_$t$REPORT_DATE.txt
	done
done

# dedicated script for net worth Assets
# Todo: add in Liabilities
# plot with SPY ticker on second y
$PNW -f NW_Assets$REPORT_DATE.txt -t SPY -og networth_example.png

# Now, let's create a breakdown of our Assets
# 'bar.py' creates a text report of the following:
#  - a breakdown of stocks/bonds/cash both by account and overall
#  - a report of investment sectors
#  - a report of individual holdings of mutual funds
# This creates another yaml directory where each commodity has a yaml
# file with info (such as sectors, holdings, fees, asset classes, etc.)
#
# Normally the -ed is optional for generating the Assets.tsv file
# HOWEVER, the bean-example files seem to generate
# bogus actual quotes for commodities, so limiting the asset quote date
# to the last full year forces the quotes to be from the ledger, not online
echo "=== Assets breakdown..."
$BCR -f ../$LEDGER -t Assets -np -ed $END_YEAR-12-31  > Assets.tsv
$BAR -f ../$LEDGER -f Assets.tsv > Assets_Report_$REPORT_DATE.txt

# Finally, we can use 'plot_things' to create time series of particular 
# accounts, I like to track the monthly expense averages of e.g. utilities 
# or food over the years.
# It will also plot cost-basis and current value of investment holdings
# It only handles one plot at a time right now, but you can dump the 
# data to a tsv file and use whatever tool to combine later

# some common Expenses
echo "=== Plots for expenses..."
for exp in 'Groceries' 'Food' 'Home' 'Salary'
do
 echo Plotting $exp...
 $PT -f ../$LEDGER -og $exp.png -a $exp -sd $START_YEAR-01-01 -ed $END_YEAR-12-31 -od $exp.tsv 
done

# How am I doing on investments?
# the -b is for balance, -cb is cost basis
echo "=== Plots for investments..."
for t in 'GLD' 'ITOT' 'VEA' 'VHT' 'RGAGX' 'VBMPX'
do
 echo Plotting $t
 $PT -f ../$LEDGER -og $t.png -a $t$ -sd $START_YEAR-01-01 -ed $END_YEAR-12-31 -b -cb 
done

