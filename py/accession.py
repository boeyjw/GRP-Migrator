import re
import os
import threading
from json import dump
from queue import Queue
from connection import Connection

#300000 because that is the near 16mb BSON limit
MAXDOCSIZE = 300000
#Adaptable Queue capacity at the cost of memory. Too low will slow the entire program
#500 is ~4.3GB at peak and average ~3.8GB memory. Perfect for 8GB + 16GB (pagefile) systems
lsjson = Queue(500)
#Thread completion indication. Popped when a thread completes its task
lscompleted = [True for x in range(0, 5)]
#List to store the entire junction tax_id in memory
lstaxId = []

class Accession(threading.Thread):
    """Extracts accession ids from SQL and translate to JSON array"""
    def __init__(self, table):
        threading.Thread.__init__(self)
        self.__conn = Connection()
        self.__cnx = self.__conn.getconn()
        self.__cursor = self.__cnx.cursor()
        self.__table = table
        self.acc = ('SELECT nne.accession, nne.`accession.version` AS version, nne.gi '
                    'FROM ncbi_{} nne INNER JOIN ncbi_nodes nn USING (tax_id) '
                    'WHERE nne.tax_id = {} AND nn.tax_id = {} {};')
        self.getdistincttaxid = ('SELECT DISTINCT tax_id FROM ncbi_{};')
        #Regex substring for version is slow
        #List filtering is extremely slow
        #Initialisation is slow
        self.__cursor.execute('SET net_write_timeout = 28800;')

    def run(self):
        """Queries and store json results into sync queue"""
        #Initialisation
        self.__cursor.execute(self.getdistincttaxid.format(self.__table))
        disttaxid = self.__cursor.fetchall()
        #Filter for tax_ids hit only
        taxids = [tuple(z)[0] for z in disttaxid if tuple(z)[0] in lstaxId]
        #Working loop
        for idd in taxids:
            try:
                self.__cursor.execute(self.acc.format(self.__table, idd, idd, ''))
                maxdoclength = self.__cursor.fetchmany(size=MAXDOCSIZE)
                while maxdoclength:
                    iddict = {
                        'ncbi_taxId': idd,
                        self.__table: [
                            {
                                'acc': str(accession),
                                #Trims off accession substrings, keeping only the version 
                                'vers': re.sub(str(accession), '', str(version)),
                                'gi': int(gi)
                            }
                            for (accession, version, gi) in maxdoclength
                        ]
                    }
                    #Put only non-empty lists into the queue
                    lsjson.put(iddict)
                    maxdoclength = self.__cursor.fetchmany(size=MAXDOCSIZE)
                print('[Normal] ' + self.__table + "\t" + str(idd))
            except MemoryError:
                #Typically this exception does not occur in 64-bit Python
                #Left here as fail safe for 32-bit Python
                if self.__cursor:
                    self.__cursor.fetchall()
                limit = MAXDOCSIZE
                offset = 0
                while True:
                    self.__cursor.execute(self.acc.format(self.__table, idd, idd, 'limit ' + str(offset) + ',' + str(limit)))
                    if not self.__cursor:
                        break
                    else:
                        iddict = {
                            'taxId': idd,
                            self.__table: [
                                {
                                    'accession': str(accession),
                                    'version': re.sub(str(accession), '', str(version)),
                                    'gi': int(gi)
                                }
                                for (accession, version, gi) in self.__cursor
                            ]
                        }
                        if iddict[self.__table]:
                            lsjson.put(iddict)
                        offset += limit
                print('[Memory error] ' + str(idd) + '\t' + str(lsjson.qsize()))
        self.closeconn()
        lscompleted.pop()

    """ DEBUG
    def run(self):
        idd = 4577
        self.__cursor.execute(self.acc.format(self.__table, idd, idd, ''))
        maxdoclength = self.__cursor.fetchmany(size=MAXDOCSIZE)
        while maxdoclength:
            print(len(maxdoclength))
            iddict = {
                'taxId': idd,
                self.__table: [
                    {
                        'acc': str(accession),
                        'vers': re.sub(str(accession), '', str(version)),
                        'gi': int(gi)
                    }
                    for (accession, version, gi) in maxdoclength
                ]
            }
            if iddict[self.__table]:
                lsjson.put(iddict)
            maxdoclength = self.__cursor.fetchmany(size=MAXDOCSIZE)
        print('[Normal] ' + self.__table + "\t" + str(idd) + '\t' + str(lsjson.qsize()))
        self.closeconn()
        lscompleted.pop()
    """

    def closeconn(self):
        """Explicitly closes connection"""
        self.__cursor.close()
        self.__conn.closeconn()

def makeidlist():
    """Store entire list of tax_ids into shared Memory"""
    conn = Connection()
    cnx = conn.getconn()
    cursor = cnx.cursor()
    cursor.execute('SELECT DISTINCT tax_id '
                   'FROM ncbi_nodes INNER JOIN gbif_ncbi_junction USING (tax_id) '
                   'ORDER BY tax_id;')
    for taxId in cursor:
        lstaxId.append(int(tuple(taxId)[0]))
    cursor.close()
    conn.closeconn()

def worker():
    """Write into a single json file in json array format"""
    wfile.write('[')
    while True:
        print("[WRITE] " + str(lsjson.qsize()))
        dump(lsjson.get(), wfile)
        if lscompleted:
            wfile.write(',')
        else:
            break
    wfile.write(']')

if __name__ == '__main__':
    makeidlist()
    wfile = open('accession.json', mode='w', buffering=16*1024, encoding='utf-8')
    tables = ['nucl_gb', 'nucl_gss', 'nucl_wgs', 'nucl_est', 'prot']
    threads = [Accession(t) for t in tables]
    twriter = threading.Thread(target=worker)

    for th in threads:
        th.daemon = True
        th.start()
    twriter.daemon = True
    twriter.start()

    for th in threads:
        th.join()
    twriter.join()

    print("File size: " + str(os.stat('accession.json').st_size))
    wfile.close()
    print('Completed')
