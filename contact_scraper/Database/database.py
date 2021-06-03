import sqlite3
import urllib.request
import sys
sys.path.append('contactDetails')


def createdb(dbname):
    conn = sqlite3.connect('{}.db'.format(dbname))
    c = conn.cursor()
    c.execute("CREATE TABLE {} (job_title TEXT)".format(str(dbname)))
    conn.commit()
    conn.close()

def insertdb(dbname, decoded_line):
    conn = sqlite3.connect('contactDetails/{}.db'.format(dbname))
    c = conn.cursor()
    c.execute("""INSERT INTO {} VALUES ('{}')""".format(dbname, decoded_line))
    conn.commit()
    conn.close()

def querydb(dbname, word):
    conn = sqlite3.connect('contactDetails/{}.db'.format(dbname))
    c = conn.cursor()
    c.execute("SELECT * FROM {} WHERE job_title = '{}'".format(dbname, word))
    #print(c.fetchall())
    words_found = c.fetchall()
    print(words_found, 'in querydb')
    conn.commit()
    conn.close()

    return words_found
"""
dbname = 'Job_names'


# create database
createdb(dbname)

# insert into database
url = "https://raw.githubusercontent.com/jneidel/job-titles/master/job-titles.txt"
file = urllib.request.urlopen(url)

lines = 0
for line in file:
    try:
        decoded_line = line.decode("utf-8").rstrip('\n')
        
        if lines % 100 == 0:
            print('On line {}'.format(lines))

        insertdb(dbname, decoded_line)

        lines += 1
    except:
        pass
"""
"""
dbname = 'Job_names'
word = 'director'
conn = sqlite3.connect('contactDetails/{}.db'.format('Job_names'))
c = conn.cursor()
c.execute("SELECT * FROM {} WHERE job_title = '{}'".format(dbname, word))
#print(c.fetchall())
words_found = c.fetchall()
conn.commit()
conn.close()
"""