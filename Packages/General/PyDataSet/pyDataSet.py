import csv
import os
import sqlite3
import re
import ConfigParser
import prettytable as PT
import LibGeneral.funcGeneral

pjoin = os.path.join
ConfPar = ConfigParser.SafeConfigParser()
gen_GUID = LibGeneral.funcGeneral.genRandomUuid

class classPyDataSet:
    def __init__(self):
        
        db_filename = "_".join(('DSDB', gen_GUID(False)))

        self.db_filepath = pjoin(os.path.dirname(os.path.realpath(__file__)), '..\..\..\Tmp\Sqlite', db_filename)
        print self.db_filepath
        self.con = sqlite3.connect(self.db_filepath)
    
    def __del__(self):
        self.con.close()
        os.remove(self.db_filepath)
        
    
    def importCSV(self, filepath):
        con = self.con
        cur = con.cursor()
        table_name =  "_".join(('DS' ,gen_GUID(False)))
        
        with open(filepath, "rb") as rawdata:
            reader = csv.reader(rawdata)
            header = rawdata.readline()
            cretate_table_stmt = "CREATE TABLE %s (%s)" %(table_name, header)
            #print cretate_table_stmt
            cur.execute(cretate_table_stmt)

            for row in reader:

                valuestr = ",".join("'%s'" % i for i in row)
                InsertQueryStr = "INSERT INTO %s (%s) VALUES (%s);" %(table_name, header, valuestr)
                #print "INSSTR :", InsertQueryStr
                cur.execute(InsertQueryStr)
                con.commit()

        return table_name

                
    def printDataSet(self, tbname):
        con = self.con
        cur = con.cursor()
        queryStmt = "SELECT * FROM %s" %(tbname)
        cur.execute(queryStmt)
        tb_desc = cur.description
        header =  map(lambda x: x[0], tb_desc)
        output_table = PT.PrettyTable(header)
        result = cur.fetchall()
        rcnt = len(result)
        print rcnt
        for row in result:

            print row
            output_table.add_row(row)
            
        print output_table

 
    def queryDataSet(self, tbname, target, criteria=None, expResCnt=None, withHeader=True, printTable=False):
        #Ardument [target] must be a list or tuple
        
        if type(target) is tuple or type(target) is list:
            con = self.con
            cur = con.cursor()
            strTargetColumn = ",".join(target)
            #print strTargetColumn
            
            if criteria is None :
                queryStmt = "SELECT %s FROM %s" %(strTargetColumn, tbname)
            elif len(criteria) > 0:
                if type(criteria) is tuple or type(target) is list:
                    strcri = ' and '.join(criteria)
                    #print strcri
                    queryStmt = "SELECT %s FROM %s WHERE %s" %(strTargetColumn ,tbname, strcri)
                else:
                    raise TypeError("classPyDataSet.queryDataSet : argument [criteria] must be tuple or list.")
                
            #print queryStmt
            cur.execute(queryStmt)
            result = cur.fetchall()
            
            if printTable is True or expResCnt is not None:
                header =  map(lambda x: x[0], cur.description)
                output_table = PT.PrettyTable(header)
            
                for row in result:
                    #print row
                    output_table.add_row(row)
                    
                if expResCnt is not None:
                    rowCnt = len(result)
                    
                    if not rowCnt == expResCnt:
                        print "classPyDataSet.queryDataSet : Dataset query result count not match expect value."
                        print output_table
                        raise Exception("classPyDataSet.queryDataSet : Dataset query result count not match expect value.")
                    elif printTable is True:
                        print output_table
                        
                elif printTable is True:
                    print output_table                        
                
            if withHeader is True:
                header =  map(lambda x: x[0], cur.description)
                result.insert(0, header)
                return result
            elif withHeader is False:
                return result
            else:
                raise ValueError("classPyDataSet.queryDataSet : argument [withHeader] must be True or False.")

            #print result

              
        else:
            raise TypeError("classPyDataSet.queryDataSet : argument [target] must be tuple or list.")
        
    def newDataSetFromQuery(self, qresult):
        #New dataset from query result
        #qresult must be a list
        con = self.con
        cur = con.cursor()        
        new_DSName = "_".join(('DS' ,gen_GUID(False)))
        header = ",".join(qresult[0])
        cretate_table_stmt = "CREATE TABLE %s (%s)" %(new_DSName, header)
        cur.execute(cretate_table_stmt)
        print header
        
        resultLenth = len(qresult)
        print resultLenth
        
        i = 1
        while i < resultLenth:
            row = qresult[i]
            valuestr = ",".join("'%s'" % n for n in row)
            print valuestr
            InsertQueryStr = "INSERT INTO %s (%s) VALUES (%s);" %(new_DSName, header, valuestr)
            print InsertQueryStr
            i = i+1
            cur.execute(InsertQueryStr)
            con.commit()
            
        return new_DSName
            
        

        
    #def newDataSet():
    
    #def appendDataSet():
        
    #def clearAllDataset():