import subprocess
from datetime import datetime as dt
from datetime import timedelta

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def run_com(args,tag='.'):
	p = subprocess.run(args,capture_output=True)
	err_lines=[]
	out_lines=[]
	if p.stderr:
		err_lines = p.stderr.decode('utf-8').split('\n')
	if p.stdout:	
		out_lines = p.stdout.decode('utf-8').split('\n')
	if len(err_lines) > 0:
		sys.stderr.write(bcolors.FAIL + tag + bcolors.ENDC + '\n')
		for l in err_lines:
			if len(l) > 0:
				sys.stderr.write(bcolors.FAIL + l + bcolors.ENDC + '\n')
	else:
		sys.stdout.write(bcolors.OKGREEN + tag +bcolors.ENDC)
	return(out_lines)

LEDGER = "../master.bc"
PRICES = "../prices.bc"
REP_PREFIX = "RNW_"

months = 3

end_date = dt.date(dt.now())
report_date = dt.date(dt.now())

results = []
dates = []

for m in range(months):
	cm = (end_date.month-1 -(months-m))%12 + 1
	yr = end_date.year - int(((months-m-1) + (12-end_date.month+1))/12) 
	sd = dt.date(dt(yr,cm,end_date.day))
	if cm < 12:
		ed = dt.date(dt(yr,cm+1,end_date.day))
	else:
		ed = dt.date(dt(yr+1,1,end_date.day))
	com = ["python","-m","beanjmw.bcr","-f",LEDGER,"-sd",sd.isoformat(),"-ed",ed.isoformat(),"-t","Assets","-pt","-pf",PRICES,"-np"]
	res = run_com(com,tag=ed.isoformat())
	for r in res:
		if "GrandTotal" in r:
			results.append(r.split('\t')[-1].strip())
			dates.append(ed.isoformat())

for d,r in zip(dates,results):
	print(d+'\t'+r)
	
