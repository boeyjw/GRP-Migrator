import io
from connection import Connection

class Junction:
    """GBIF NCBI junction"""
    def __init__(self):
        self.__conn = Connection()
        self.__cnx = self.__conn.getconn()
        self.__cursor = self.__cnx.cursor(buffered=True)
        self.query = ('SELECT gt.taxonID, nn.tax_id '
                      'FROM {} gt CROSS JOIN {} nn ON gt.{} = nn.{} '
                      'WHERE gt.{} = \'{}\';')
        self.query = ('SELECT gt.taxonID, nn.tax_id '
                      'FROM {} gt CROSS JOIN ncbi_names nn ON gt.{} = nn.unique_name '
                      'WHERE gt.{} LIKE \'%[{}]\';')
        self.__cquery = ('CREATE TABLE IF NOT EXISTS gbif_ncbi_junction ('
                         'taxonID INT(10) UNSIGNED NOT NULL,'
                         'tax_id MEDIUMINT(11) UNSIGNED NOT NULL);')

    def closeconn(self):
        self.__cursor.close()
        self.__conn.closeconn()

    def __rf(self):
        """Read all lines in file input"""
        with open('cvn_nuq.comm', mode='r') as rfile:
            names = [lines.split('\n') for lines in rfile.readlines()]
        return names

    def __selectstmt(self, gt, nn, gton, nnon, gtparam):
        self.__cursor.execute(self.query.format(gt, nn, gton, nnon, gton, gtparam))
        list = [(taxonID, tax_id) for (taxonID, tax_id) in self.__cursor]
        return list

    def runjunction(self):
        self.__cursor.execute(self.__cquery)
        names = self.__rf()
        with open('gbif_ncbi_junction.csv', mode='w', encoding='utf-8') as wfile:
            for lines in names:
                res = self.__selectstmt('gbif_taxon', 'ncbi_names', 'canonicalName', 'name_txt', lines)
                if not res:
                    res = self.__selectstmt('gbif_taxon', 'ncbi_names', 'canonicalName', 'unique_name')
