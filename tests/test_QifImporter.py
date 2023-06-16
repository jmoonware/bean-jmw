from qifparse.parser import QifParser

qif_example1='test1.qif'

def test_Basic():
	with open(qif_example1,'r') as f:
		qf = QifParser(dayfirst=False).parse(f)
	assert qf
