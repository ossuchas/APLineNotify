# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import uuid
import shutil
import pyodbc
import os
import sys
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


def plotCombineLineBarChart(source_path):
    # Get date value from config
    date_list = get_date()

    start_date = '2019-01-01'
    end_date = '2019-01-31'
    curr_year = date_list[2]
    curr_month_text = date_list[3]
    curr_date_no = date_list[5]

    # Bar Chart
    df0 = selSumGroupByType(start_date, end_date)
    # df0 = pd.read_excel('Data2.xlsx', sheet_name='Sheet1')

    # Line
    df1 = selSumGroupBy(start_date, end_date)
    # df1 = pd.read_excel('Data2.xlsx', sheet_name='Sheet2')

    total_req = df1['VOL'].sum()
    avg = df1["VOL"].mean()
    avg = str(round(avg)).replace('.0', '')

    # fig, ax = plt.subplots(figsize=(15, 10))
    ax = df0.groupby(['DATE', 'TYPE']).sum().unstack().plot(kind='bar', stacked=True)
    plt.legend(('Main[9]', 'Contract[2]'), loc='best')
    ax = df1['VOL'].plot(kind='line', marker='d')
    # plt.plot(df1['VOL'],  marker='D')

    plt.ylabel('Total Request')
    plt.xlabel('Date Of Month')

    plt.xticks(range(0, 31), fontsize=8)
    plt.yticks(fontsize=8)

    title = "Summary Report Web Vendor II\n Jan-2019"
    plt.title(title)

    # Annotate Text
    x_offset = 0
    y_offset = 0.03

    for p in ax.patches:
        b = p.get_bbox()
        val = "{:.0f}".format(b.y1 - b.y0)
        ax.annotate(val, ((b.x0 + b.x1) / 2 + x_offset, b.y1 + y_offset))

    textstr = "Total " + "{:,}".format(total_req) + " Reqs.\n" \
              "Avg. " + "{}".format(avg) + " Reqs./Day"

    # these are matplotlib.patch.Patch properties
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)

    # place a text box in upper left in axes coords
    # ax.text(0.80, 0.90, textstr, transform=ax.transAxes, fontsize=10, verticalalignment='top', bbox=props)
    ax.text(0.025, 0.80, textstr, transform=ax.transAxes, fontsize=10, verticalalignment='top', bbox=props)

    # source_path = '/home/ubuntu/myPython/LineNotify'
    filename = '{}.png'.format(str(uuid.uuid4().hex))
    fileFullPath = "{}/{}".format(source_path, filename)
    print(fileFullPath)

    plt.savefig(fileFullPath, bbox_inches='tight')
    plt.show()
    return filename


def copy2shareImg(source_path, dest_path, filename):
    # Copy File to Share Folder
    if get_platform() == 'win32':
        shutil.copy2(source_path + '\\' + filename, dest_path + '\\' + filename)
    else:
        shutil.copy2(source_path + '/' + filename, dest_path + '/' + filename)


def selSumGroupByType(start_date, end_date):

    sqlconn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.2.110;PORT=1443;DATABASE=WebVendor_V2;UID=sa;PWD=sql@apthai;')
    strSQL = "SELECT  CAST(FORMAT(a.CreateDate, 'dd') AS INT) AS DATE , \
            CASE WHEN d.Trans_Type = '2' THEN 'งานเหมา' \
                 WHEN d.Trans_Type = '9' THEN 'งานหลัก' \
                 WHEN d.Trans_Type = '1' THEN 'ค่าของ' \
                 ELSE 'ค่าของ' \
            END AS TYPE , \
            COUNT(*) VOL \
    FROM    dbo.WD_PO_Delivery a , \
            dbo.WD_PO_Delivery_Item b \
            LEFT JOIN dbo.WD_InterfaceQIS c ON b.TRAN_ID = c.TRAN_ID \
            LEFT JOIN dbo.WD_InterfaceQIS_Detail d ON c.TRAN_ID = d.TRAN_ID \
                                                      AND b.POID = d.POID \
                                                      AND CAST(b.ItemNO AS INT) = CAST(d.ItemNO AS INT) \
    WHERE   a.TRAN_ID = b.TRAN_ID \
            AND CAST(a.CreateDate AS DATE) BETWEEN '{}' \
                                           AND     '{}' \
            AND d.Trans_Type <> '1' \
    GROUP BY CAST(FORMAT(a.CreateDate, 'dd') AS INT) , \
            CASE WHEN d.Trans_Type = '2' THEN 'งานเหมา' \
                 WHEN d.Trans_Type = '9' THEN 'งานหลัก' \
                 WHEN d.Trans_Type = '1' THEN 'ค่าของ' \
                 ELSE 'ค่าของ' \
            END \
    ORDER BY 1".format(start_date, end_date)

    return pd.read_sql(strSQL, sqlconn)
    # return pd.read_excel('Data2.xlsx', sheet_name='Sheet1')


def selSumGroupBy(start_date, end_date):
    sqlconn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.2.110;PORT=1443;DATABASE=WebVendor_V2;UID=sa;PWD=sql@apthai;')
    strSQL = "SELECT  CAST(FORMAT(a.CreateDate, 'dd') AS INT) AS DATE , \
            COUNT(*) VOL \
    FROM    dbo.WD_PO_Delivery a , \
            dbo.WD_PO_Delivery_Item b \
            LEFT JOIN dbo.WD_InterfaceQIS c ON b.TRAN_ID = c.TRAN_ID \
            LEFT JOIN dbo.WD_InterfaceQIS_Detail d ON c.TRAN_ID = d.TRAN_ID \
                                                      AND b.POID = d.POID \
                                                      AND CAST(b.ItemNO AS INT) = CAST(d.ItemNO AS INT) \
    WHERE   a.TRAN_ID = b.TRAN_ID \
            AND CAST(a.CreateDate AS DATE) BETWEEN '{}' \
                                           AND     '{}' \
            AND d.Trans_Type <> '1' \
    GROUP BY CAST(FORMAT(a.CreateDate, 'dd') AS INT) \
    ORDER BY 1".format(start_date, end_date)

    return pd.read_sql(strSQL, sqlconn)
    # return pd.read_excel('Data2.xlsx', sheet_name='Sheet2')


def execInstLineNotify(dest_path, filename):

    if get_platform() == 'win32':
        fileFullPath = dest_path + '\\' + filename
    else:
        fileFullPath = dest_path + '/' + filename

    myConnDB = ConnectDB()
    # params = ('WEBVENDOR','WebVendorAutoBot','0830824173','Web Vendor Summary Report', filename,'','WDLineChartNotify')
    params = ('PERSONAL','WebVendorAutoBot','0830824173','Web Vendor Summary Report', fileFullPath,'','WDLineChartNotify')
    # params = ('WEBVENDOR1','WebVendorAutoBot','0830824173','Web Vendor Summary Report', fileFullPath,'','WDLineChartNotify')

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
    print(dfltVal)

    # Plot Stacked Bar Chart
    filename = plotCombineLineBarChart(dfltVal[0])

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
