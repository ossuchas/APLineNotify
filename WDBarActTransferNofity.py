# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import calendar
import uuid
import shutil
import pyodbc
from datetime import datetime
import os
import sys
import logging
import numpy as np


class ConnectDB:
    def __init__(self):
        ''' Constructor for this class. '''
        self._connection = pyodbc.connect(
            # 'Driver={SQL Server};Server=192.168.2.58;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;')
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.2.58;PORT=1443;DATABASE=db_iconcrm_fusion;UID=iconuser;PWD=P@ssw0rd;', autocommit=True)
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


def plotBar(source_path):
    # Get date value from config
    date_list = get_date()

    start_date = date_list[0]
    end_date = date_list[1]
    curr_year = date_list[2]
    curr_month_text = date_list[3]
    curr_date_no = date_list[5]

    sqlconn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.2.58;PORT=1443;DATABASE=db_iconcrm_fusion;UID=iconuser;PWD=P@ssw0rd;')
    strSQL = """
    SELECT [MonthTransfer], [ProjectType],
       SUM([SellingPrices]) AS SellingPrices,
       SUM([PricesM]) AS SellingPricesM
    FROM [dbo].[vw_SumTransferByProj]
    GROUP BY [MonthTransfer], [ProjectType]
    """

    df = pd.read_sql(strSQL, sqlconn)

    total_prices = df['SellingPricesM'].sum()
    df_sum_bg = df.groupby(['ProjectType'], sort=True).sum()
    total_cd1 = df_sum_bg.iloc[0][2].astype(np.float)
    total_cd2 = df_sum_bg.iloc[1][2].astype(np.float)
    total_sh = df_sum_bg.iloc[2][2].astype(np.float)
    total_th = df_sum_bg.iloc[3][2].astype(np.float)

    df = df.set_index('MonthTransfer')

    ax = df.set_index('ProjectType', append=True)['SellingPricesM'].unstack().plot.bar(stacked=False, figsize=(10, 8))

    plt.legend(loc='best')

    plt.ylabel('Total Selling Prices')
    plt.xlabel('Month of Actual Transfer')

    plt.xticks(fontsize=8)
    plt.yticks(fontsize=8)

    title = "Summary Report Actual Transfer by Business Group (Million baht)\n Since 01-Jan-2019 to {}-{}-{}".format(curr_date_no, curr_month_text, curr_year)
    plt.title(title)

    x_offset = -0.03
    y_offset = 0.02

    i_count = 1
    count_row = int(curr_date_no)

    for p in ax.patches:
        b = p.get_bbox()
        if i_count <= count_row:
            val = "{:.2f}M".format(b.y1 + b.y0)
        else:
            val = "{:.2f}M".format(b.y1 - b.y0)

        val2 = ((b.x0 + b.x1)/2 + x_offset, b.y1 + y_offset)
        ax.annotate(val, ((b.x0 + b.x1)/2 + x_offset, b.y1 + y_offset))
        i_count += 1

    textstr = "CD1 {} = ".format("").ljust(8) + "{:,.2f}M".format(total_cd1) + \
              "\nCD2 {} = ".format("").ljust(8) + "{:,.2f}M".format(total_cd2) + \
              "\nSH {} = ".format("").ljust(8) + "{:,.2f}M".format(total_sh) + \
              "\nTH {} = ".format("").ljust(8) + "{:,.2f}M".format(total_th) + \
              "\n-------------------------\n" + \
              "Total = " + "{:,.2f}M".format(total_prices)

    # these are matplotlib.patch.Patch properties
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)

    # place a text box in upper left in axes coords
    ax.text(0.80, 0.80, textstr, transform=ax.transAxes, fontsize=10, verticalalignment='top', bbox=props)

    filename = '{}.png'.format(str(uuid.uuid4().hex))
    fileFullPath = "{}/{}".format(source_path, filename)
    #print(filename)

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

    # params = ('WEBVENDOR','WebVendorAutoBot','0830824173','Web Vendor Summary Report', filename,'','WDLineChartNotify')
    params = ('PERSONAL', 'WebVendorAutoBot', '0830824173', 'Summary Report Actual Transfer by Business Group',
              fileFullPath, '', 'WDLineChartNotify')

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
    filename = plotBar(dfltVal[0])

    # source_path = r"C:\Users\suchat_s\Dropbox\AP\Project\96_Python\1Develop\1_3Technical Document\WDStackedBarNotify"
    # dest_path = r"\\192.168.2.52\dev2\temp\img"
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

