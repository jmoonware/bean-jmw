# script to generate financial reports from ledger
# Generates Expense and Income reports using the settings in bcr.tsv
# Note that bcr.tsv gets updated each time by default as new categories appear
# Edit the bcr.tsv file if needed to change inclusion status or months average
LEDGER=../ledger.beancount
# Created by bci from ledger; list of assets in tsv format 
ASSET_LIST=asset_list.tsv
# Created by bar - text report of asset breakdown by categories
# Import this into a spreadsheet to plot
ASSET_REPORT=asset_report.txt
START_DATE=2023-01-01
END_DATE=2023-12-31
REPORT_DATE=`date -Idate`
# Expense report between above dates for ledger
python -m beanjmw.bcr -f $LEDGER -sd $START_DATE -ed $END_DATE -t Expenses -ma -dt -html > Expenses_$REPORT_DATE.html
# Income report between above dates for ledger
python -m beanjmw.bcr -f $LEDGER -sd $START_DATE -ed $END_DATE -t Income -ma -dt -html > Income_$REPORT_DATE.html
# Asset reports for above ledger
python -m beanjmw.bcr -f $LEDGER -t Assets -np > $ASSET_LIST
python -m beanjmw.bcr -f $LEDGER -t Assets -html > Assets_$REPORT_DATE.html
python -m beanjmw.bar -f $ASSET_LIST --advisor_fee 0.95 > $ASSET_REPORT

