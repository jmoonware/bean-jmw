import subprocess
from datetime import datetime as dt
from datetime import timedelta
import argparse
import sys
import matplotlib.pyplot as plt
import numpy as np

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

default_ledger = "../master.bc"
default_prices = "../prices.bc"
default_monthly_interval = 1
default_plot_top = -1

ap = argparse.ArgumentParser()
ap.add_argument('-f','--ledger_file',required=False,help='Ledger file (default = {0}'.format(default_ledger),default=default_ledger)
ap.add_argument('-pf','--price_file',required=False,help='Prices file (default = {0})'.format(default_prices),default=default_prices)
ap.add_argument('-og','--output_graph',required=False,help='Name of plot image file to save - otherwise plot to screen',default='')
ap.add_argument('-od','--output_data',required=False,help='Name of tsv file of plot data to save',default='')
ap.add_argument('-sd','--start_date',required=False,help='Starting date (iso format, default one year ago)',default=dt.date(dt.now()-timedelta(days=365)).isoformat())
ap.add_argument('-ed','--end_date',required=False,help='End date (iso format, default today)',default=dt.date(dt.now()).isoformat())
ap.add_argument('-mi','--monthly_interval',required=False,help='Interval between points (months, default = {0})'.format(default_monthly_interval),default=default_monthly_interval)
ap.add_argument('-pt','--plot_top',required=False,help='Plot only this many top-valued instition (aggregate the rest into "other", 0 for plot all, negative for plot only total, default {0})'.format(default_plot_top),default=default_plot_top)

clargs = ap.parse_args(sys.argv[1:])

report_date = dt.date(dt.now())

results = []
dates = []
by_institution={}

end_date = dt.date(dt.fromisoformat(clargs.end_date))
start_date = dt.date(dt.fromisoformat(clargs.start_date))

months = int(round(((end_date-start_date).days/30.4375)))

for m in range(0,months,int(clargs.monthly_interval)):
	cm = (end_date.month-1 -(months-m))%12 + 1
	yr = end_date.year - int(((months-m-1) + (12-end_date.month+1))/12) 
	sd = dt.date(dt(yr,cm,end_date.day))
	if cm < 12:
		ed = dt.date(dt(yr,cm+1,end_date.day))
	else:
		ed = dt.date(dt(yr+1,1,end_date.day))
	com = ["python","-m","beanjmw.bcr","-f",clargs.ledger_file,"-sd",sd.isoformat(),"-ed",ed.isoformat(),"-t","Assets","-pt","-pf",clargs.price_file,"-np"]
	res = run_com(com,tag=ed.isoformat())
#	print(res)
	for r in res[1:]:
		toks = r.split('\t')
		if "GrandTotal" in r:
			results.append(toks[-1].strip())
			dates.append(ed)
		elif not 'TOTAL' in r and len(toks)==4:
			inst = toks[0]
			if not inst in by_institution:
				by_institution[inst]={}
			if not ed in by_institution[inst]:
				by_institution[inst][ed]=0
			by_institution[inst][ed]+=float(toks[-1])

# header line
header_line = ['date','Total']
[header_line.append(inst) for inst in by_institution]
print('\t'.join(header_line))

plots = [[]]
[plots.append([]) for inst in by_institution]

for d,r in zip(dates,results):
	l = [d.isoformat(),r]
	plots[0].append(float(r))
	for idx, inst in enumerate(by_institution.keys()):
		if d in by_institution[inst]:
			l.append("{0:.2f}".format(by_institution[inst][d]))
			plots[idx+1].append(by_institution[inst][d])
		else:
			l.append('0.00')
			plots[idx+1].append(0.0)
	print('\t'.join(l))

plt.plot(dates,plots[0],label="Total")

if int(clargs.plot_top) > 0: # only plot these top N institutions
	dplots = np.array(plots)
	print(dplots[:,-1]) # latest values
	max_cols = np.argsort(dplots[:,-1])[::-1]
	print(max_cols)
	other = np.zeros(len(dplots[0]))
	for idx, max_idx in enumerate(max_cols[1:]):
		# max_cols includes the first 'total' col, so indices off by one 
		inst = list(by_institution.keys())[max_idx-1]
		if idx < int(clargs.plot_top):
			plt.plot(dates,plots[max_idx],label=inst)	
		else: # rest of institutions
			other+=plots[max_idx]
	plt.plot(dates,other,label="Other")

elif int(clargs.plot_top)==0: # plot everything
	for idx, inst in enumerate(by_institution.keys()):
		plt.plot(dates,plots[idx+1],label=inst)	

plt.legend()
plt.xlabel("Date")
plt.ylabel("Value (USD)")
plt.show()
