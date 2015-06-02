import sqlite3 as db

con = db.connect('/dev/shm/adapt.db')

with con:
	cur = con.cursor()
	
	try:
		cur.execute('DROP TABLE Data')
	except db.OperationalError:
		pass
		
	cur.execute('CREATE TABLE Data(ID TEXT, Chunk INT, Data TEXT)')
	cur.execute('CREATE UNIQUE INDEX DataIndex ON Data(ID, Chunk)')	
	con.commit()

con.close()
