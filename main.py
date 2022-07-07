import sqlite3
import ssl
from bs4 import BeautifulSoup #You need to have beautiful soup in order for this code to work
from urllib.request import urlopen
from urllib.parse import urlparse
from urllib.parse import urljoin

# In order to ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


con = sqlite3.connect('database.sqlite')
cur = con.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS Pages
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
     error INTEGER)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Webs (urls TEXT UNIQUE)''')

# In order to check whether we already have any URLs in the database
cur.execute('''SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1''')
check = cur.fetchone()
if check is not None:
    print("Prior progress found, resuming the crawl. In order to start a new crawl, delete database.sqlite ")
else:
    link = input('Enter the URL of the page you wish to crawl: ')
    if (len(link) < 1):
        print('ERROR1')
        link = input('Enter a valid URL')
    if (link.endswith('/')) : link = link[ :-1]
    validlink = link
    if (link.endswith('.html' or '.htm') ):
        endpos = link.rfind('/')
        validlink = link[ :endpos]

    if(len(validlink) > 1):
        cur.execute ('''INSERT OR IGNORE INTO Webs (urls) VALUES (?)''', (validlink, ))
        cur.execute('INSERT OR IGNORE INTO Pages (url, html) VALUES ( ?, NULL)', (validlink,))
        con.commit()

# In order to get the pre-existing URLs
cur.execute('''SELECT urls FROM Webs''')
links = list()
for a in cur:
    links.append(str(a[0]))

# We now ask the user as to how many URLs he wants to collect.
rvar = 0
while True:
    if (rvar<1) :
        desiredurlcount = input('Enter the number of pages you wish to crawl through:')
        if (len(desiredurlcount) <1): break
        rvar = int(desiredurlcount)
    rvar -= 1

    cur.execute('SELECT url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
    try:
        check = cur.fetchone()
        url = check[0]
    except:
        print('No more pages to retrieve')
        rvar = 0
        break

    try:
        handle = urlopen(url, context=ctx)
        data = handle.read()
        if (handle.getcode() != 200):
            print('Error:', handle.getcode())
            cur.execute('UPDATE Pages SET error=? WHERE url=?', (handle.getcode(), url))

        if handle.info().get_content_type() != 'text/html':
            cur.execute('DELETE FROM Pages WHERE url=?', (url,))
            print('Does not contain Text or HTML')
            con.commit()
            continue
        soup = BeautifulSoup(data, "html.parser")
    except KeyboardInterrupt:
        print('Program interrupted')
        break
    except:
        print("ERROR2")
        cur.execute('UPDATE Pages SET error = -420 WHERE url=?', (url, ))
        con.commit()
        continue

    cur.execute('INSERT OR IGNORE INTO Pages (url, html) VALUES ( ?, NULL)', (url,))
    cur.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(data), url))
    con.commit()

    anchortags = soup('a')

    for anchortag in anchortags:
        hypertextref = anchortag.get('href', None)
        if (hypertextref is None):
            continue
        a = urlparse(hypertextref)
        if (len(a.scheme) <1):
            hypertextref = urljoin(url, hypertextref)
        vallink = hypertextref.find('#')
        if ( vallink > 1 ) : hypertextref = hypertextref[ :vallink]
        if ( hypertextref.endswith('.png') or hypertextref.endswith('.jpg') or hypertextref.endswith('.gif') ) : continue
        if ( hypertextref.endswith('/') ) : hypertextref = hypertextref[:-1]
        if (len(hypertextref) < 1): continue


# Check if the URL is in any of the webs
        found = False
        for lnk in links:
            if (hypertextref.startswith(lnk)) :
                found = True
                break
        if not found: continue

        cur.execute('INSERT OR IGNORE INTO Pages (url, html) VALUES ( ?, NULL)', (hypertextref, ) )
        con.commit()

cur.close()






