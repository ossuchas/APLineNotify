from __future__ import unicode_literals
import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd
import calendar
import uuid
import shutil
import pyodbc
from datetime import datetime, timedelta
import os
import sys
import logging
from sqlalchemy import create_engine
import urllib


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
    params = 'Driver={ODBC Driver 17 for SQL Server};Server=192.168.0.28;Database=ITHelpDesk_Test;uid=sa;pwd=P@ssw0rd;'
    params = urllib.parse.quote_plus(params)
    db = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params, fast_executemany=True)

    sql_query = """
    SELECT CASE
               WHEN S.MasterCode = 'SYS-QIS (แนวสูง)' THEN
                   'SYS-QIS'
               ELSE
                   S.MasterCode
           END AS MasterCode,
           CAST(T.RequestDate AS DATE) AS RequestDate,
           st.MasterDescription AS 'Status',
           ISNULL(MType.MasterDescription, 'Waiting') AS 'JobType',
           COUNT(*) AS TotalRecord
    FROM dbo.TRequestDoc T WITH (NOLOCK)
        INNER JOIN dbo.MMaster S WITH (NOLOCK)
            ON T.SysId = S.MasterCode
        LEFT JOIN dbo.MCategories MC WITH (NOLOCK)
            ON MC.CategoryId = T.CategoryId
        LEFT JOIN dbo.MMaster MType WITH (NOLOCK)
            ON MType.MasterCode = T.JobTypeId
        LEFT JOIN dbo.MMaster st WITH (NOLOCK)
            ON T.RequestStatusId = st.MasterCode
        LEFT JOIN dbo.vw_UserAll VWUser
            ON VWUser.UserID = T.RequestUserId
        LEFT JOIN dbo.vw_Employee VWEmp
            ON VWEmp.EmpID = VWUser.UserName
    WHERE S.MasterCode NOT IN ( 'E-mail', 'SYS-ESS', 'SYS-FingerScan', 'Printer/อุปกรณ์อื่นๆ', 'PC/Notebook/Tablet',
                                'Internet / Network / Wireless', 'Program ทั่วไป', 'Other', 'Server', 'Sharefile/FTP'
                              )
          AND CAST(T.RequestDate AS DATE)
          BETWEEN DATEADD(DAY, -7, CONVERT(DATE, GETDATE())) AND DATEADD(DAY, -1, CONVERT(DATE, GETDATE()))
          AND ISNULL(T.IsDelete, 0) <> 1
    GROUP BY CASE
                 WHEN S.MasterCode = 'SYS-QIS (แนวสูง)' THEN
                     'SYS-QIS'
                 ELSE
                     S.MasterCode
             END,
             CAST(T.RequestDate AS DATE),
             st.MasterDescription,
             ISNULL(MType.MasterDescription, 'Waiting')
    ORDER BY CAST(T.RequestDate AS DATE) DESC
    """
    df = pd.read_sql(sql=sql_query, con=db)

    mpl.style.use(['ggplot']) # optional: for ggplot-like style
    # ax = df.groupby(["MasterCode"])['TotalRecord'].size().plot(kind='bar', figsize=(18, 8))
    ax = df.groupby("MasterCode").sum().sort_values("TotalRecord", ascending=False).head(10).plot(kind='bar', figsize=(10, 8))
    plt.ylabel('Total Request', fontsize=15, fontweight='black', color='#333F4B')
    plt.xlabel('System',fontsize=15, fontweight='black', color='#333F4B')

    plt.xticks(fontsize=9,rotation=45)
    plt.yticks(fontsize=8)

    first_date = datetime.now() - timedelta(days=7)
    yesterday = datetime.now() - timedelta(days=1)

    end_date = yesterday.strftime('%d/%m/%Y')
    start_date = first_date.strftime('%d/%m/%Y')

    plt.title("Summary Job Helpdesk for AP's System Top 10\n Since {} - {}".format(start_date, end_date))

    x_offset = -0.15
    y_offset = 0.25

    for p in ax.patches:
        b = p.get_bbox()
        val = "{:.0f}".format(b.y1 - b.y0)
        ax.annotate(val, ((b.x0 + b.x1)/2 + x_offset, b.y1 + y_offset), size=11)

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
    params = ('PERSONAL', 'WebVendorAutoBot', '0830824173', "Summary Job Helpdesk for AP's System Top 10 last week", fileFullPath, '', 'WDLineChartNotify')
    # params = ('ITHEAD', 'ITHEAD', '0830824173',"Summary Job Helpdesk for AP's System Top 10 last week", fileFullPath, '', 'LineChartNotify')

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
    filename = plotBar(dfltVal[0])

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

