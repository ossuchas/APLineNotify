# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import uuid
import shutil
import pyodbc
import os
import sys
import logging
from datetime import datetime


class ConnectDB:
    def __init__(self):
        ''' Constructor for this class. '''
        self._connection = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.2.58;PORT=1443;DATABASE=db_iconcrm_fusion;UID=iconuser;PWD=P@ssw0rd;')
        #'Driver={SQL Server};Server=192.168.2.58;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;')
        self._cursor = self._connection.cursor()

    def query(self, query):
        try:
            result = self._cursor.execute(query)
        except Exception as e:
            logging.error('error execting query "{}", error: {}'.format(query, e))
            return None
        finally:
            return result

    def update(self, sqlStatement):
        try:
            self._cursor.execute(sqlStatement)
        except Exception as e:
            logging.error('error execting Statement "{}", error: {}'.format(sqlStatement, e))
            return None
        finally:
            self._cursor.commit()

    def exec_sp(self, sqlStatement, params):
        try:
            self._cursor.execute(sqlStatement, params)
        except Exception as e:
            logging.error('error execting Statement "{}", error: {}'.format(sqlStatement, e))
            return None
        finally:
            self._cursor.commit()

    def exec_spRet(self, sqlStatement, params):
        try:
            result = self._cursor.execute(sqlStatement, params)
        except Exception as e:
            print('error execting Statement "{}", error: {}'.format(sqlStatement, e))
            return None
        finally:
            return result

    def __del__(self):
        self._cursor.close()


def get_date():
    """
    0 : start date (Ex. 2019-02-01)
    1 : end date (Ex. 2019-02-17)
    2 : year (Ex. 2019)
    3 : month (Jan, Feb, ...)
    4 : month of number (1,2,3,4,5,...12)
    5 : current date number (1,2,3,...31)
    :return:
    """
    date_list = []

    # Start Date
    date_list.append(datetime.today().replace(day=1).strftime('%Y-%m-%d'))
    # End Date
    date_list.append(datetime.now().strftime('%Y-%m-%d'))
    # Current Year, 2019
    date_list.append(str(datetime.now().year))
    # Current Month, Jan, Feb, ...
    date_list.append(datetime.now().strftime('%h'))

    # Current Month, 1,2,3,4,..12
    date_list.append(str(int(datetime.now().strftime('%m'))))

    # Current Date, 1,2,3,4,..31
    date_list.append(str(int(datetime.now().strftime('%d'))))

    return date_list


def get_platform():
    if sys.platform == 'win32':
        return 'win32'
    else:
        return 'linux'


def getDfltParam():
    """
    index value
    0 = Source Path
    1 = Destination Path
    """

    platform = get_platform()

    strSQL = """
    SELECT param_vlue
    FROM dbo.CRM_Param
    WHERE param_code = 'LINE_NOTIFY'
    AND param_data = '{}'
    ORDER BY param_seqn
    """.format(platform)

    myConnDB = ConnectDB()
    result_set = myConnDB.query(strSQL)
    returnVal = []

    for row in result_set:
        returnVal.append(row.param_vlue)

    return returnVal


def displayText(pct, sizes):
    absolute = round((pct / 100. * np.sum(sizes)),2)
    return "{:.2f}%\n({:.2f} GB)".format(pct, absolute)


def plotPieChart(source_path):
    # Get date value from config
    date_list = get_date()

    curr_year = date_list[2]
    curr_month_text = date_list[3]
    curr_month_no = date_list[5]

    sqlconn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.2.110;PORT=1443;DATABASE=WebVendor_V2;UID=sa;PWD=sql@apthai;')
    strSQL = "SELECT CASE \
	when Doc_Type = 'IV' THEN 'Invoice Doc.' \
	WHEN doc_Type = 'PO' THEN 'Request Doc.' \
	END AS TYPE, \
    SUM(CAST(Doc_SizeByte AS BIGINT)) AS VOL \
    FROM dbo.WD_Doc_Delivery \
    WHERE Doc_SizeByte IS NOT NULL \
    AND Doc_Type IN ('PO','IV') \
    GROUP BY Doc_Type \
    UNION ALL \
    SELECT 'Free Disk Space.' AS TYPE, \
    253513485103 - SUM(CAST(Doc_SizeByte AS BIGINT)) AS VOL \
    FROM dbo.WD_Doc_Delivery \
    WHERE Doc_SizeByte IS NOT NULL \
    AND Doc_Type IN('PO', 'IV') "

    df = pd.read_sql(strSQL, sqlconn)

    labels = df['TYPE']
    sizes = df['VOL']/1024/1024/1024
    explode = (0.1, 0.1, 0.1)  # only "explode" the 2nd slice (i.e. 'Hogs')

    fig1, ax1 = plt.subplots()

    wedges, texts, autotexts = ax1.pie(sizes, explode=explode, labels=labels,
                                   autopct=lambda p: displayText(p, sizes),
                                   shadow=True, startangle=90, textprops=dict(color="w", fontsize="12"))

    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    ax1.legend(wedges, labels,
           title="Document Type",
           loc="center left",
           bbox_to_anchor=(1, 0, 0.5, 1))

    title = "Web Vendor\n Summary Report for Document Disk Usage\n at {}-{}-{}".format(curr_month_no, curr_month_text, curr_year)
    ax1.set_title(title, bbox={'facecolor':'0.8', 'pad':5})

    filename = '{}.png'.format(str(uuid.uuid4().hex))
    fileFullPath = "{}/{}".format(source_path, filename)

    plt.savefig(fileFullPath, bbox_inches='tight')
    plt.show()
    return filename


def copy2shareImg(source_path, dest_path, filename):
    # Copy File to Share Folder
    if get_platform() == 'win32':
        shutil.copy2(source_path + '\\' + filename, dest_path + '\\' + filename)
    else:
        shutil.copy2(source_path + '/' + filename, dest_path + '/' + filename)


def execInstLineNotify(dest_path, filename):

    if get_platform() == 'win32':
        fileFullPath = dest_path + '\\' + filename
    else:
        fileFullPath = dest_path + '/' + filename

    myConnDB = ConnectDB()
    # params = ('PERSONAL','WebVendorAutoBot','0830824173','Web Vendor Document Disk Usage', fileFullPath,'','WDLineChartNotify')
    params = ('WEBVENDOR1','WebVendorAutoBot','0830824173','Web Vendor Document Disk Usage', fileFullPath,'','WDLineChartNotify')
    
    sqlStmt = """
    EXEC dbo.sp_InstLineNotifyMsg @LineToken = ?,
        @LineOwner = ?,
        @LineMobile = ?,
        @LineMsg = ?,
        @FilePath = ?,
        @Remarks = ?,
        @CreateBy = ?
    """
    myConnDB.exec_sp(sqlStmt, params)
    
    # For Infra Monitor
    params = ('INFRA','InfraAutoBot','0830824173','Web Vendor Document Disk Usage', fileFullPath,'','WDLineChartNotify')
    
    sqlStmt = """
    EXEC dbo.sp_InstLineNotifyMsg @LineToken = ?,
        @LineOwner = ?,
        @LineMobile = ?,
        @LineMsg = ?,
        @FilePath = ?,
        @Remarks = ?,
        @CreateBy = ?
    """
    myConnDB.exec_sp(sqlStmt, params)


def deleteFile(source_path, filename):

    if get_platform() == 'win32':
        if os.path.exists(source_path + '\\' + filename):
            os.remove(source_path + '\\' + filename)
        else:
            print("The file does not exist")
    else:
        if os.path.exists(source_path + '/' + filename):
            os.remove(source_path + '/' + filename)
        else:
            print("The file does not exist")


def main():

    dfltVal = getDfltParam()
    # print(dfltVal)

    # Plot Stacked Bar Chart
    filename = plotPieChart(dfltVal[0])

    source_path = dfltVal[0]
    dest_path = dfltVal[1]

    # Copy Image file to Network Folder
    copy2shareImg(source_path, dest_path, filename)

    # Delete source file
    deleteFile(source_path, filename)

    #Insert Msg to  Table Line Notify
    execInstLineNotify(dest_path, filename)


if __name__ == '__main__':
    main()
