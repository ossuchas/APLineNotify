#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import requests
import pyodbc
import logging

def _lineNotify(token, msg, fileimage):
    LINE_ACCESS_TOKEN = token
    url = "https://notify-api.line.me/api/notify"
    if fileimage:
        file = {'imageFile': open(fileimage, 'rb')}
    else:
        file = None
    data = ({
        'message': msg
    })
    LINE_HEADERS = {"Authorization": "Bearer "+LINE_ACCESS_TOKEN}
    session = requests.Session()
    return session.post(url, headers=LINE_HEADERS, files=file, data=data)

class ConnectDB:
    def __init__(self):
        ''' Constructor for this class. '''
        self._connection = pyodbc.connect(
            # 'Driver={SQL Server};Server=192.168.2.58;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;')
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.2.58;PORT=1443;DATABASE=db_iconcrm_fusion;UID=iconuser;PWD=P@ssw0rd;')
        self._cursor = self._connection.cursor()

    def query(self, query):
        try:
            result = self._cursor.execute(query)
        except Exception as e:
            logging.error('error execting query "{}", error: {}'.format(query, e))
            print('error execting query "{}", error: {}'.format(query, e))
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


def main():

    strSQL = """
    SELECT Tran_ID ,Line_Token ,Line_Owner ,Line_MobileNoOwner ,Send_Msg
      ,File_Path ,Remarks ,Send_Status ,Send_Date ,
      CreateDate ,CreateBy ,ModifyDate ,ModifyBy
    FROM dbo.CRM_LineNotify
    WHERE Send_Status IN ('W', 'P')
    ORDER BY CreateDate
        """

    myConnDB = ConnectDB()
    myConnDBStatus = ConnectDB()
    result_set = myConnDB.query(strSQL)

    for row in result_set:
        token = row.Line_Token.strip()
        msg = row.Send_Msg

        # Update Processing Send_Status = 'P'
        params = (row.Tran_ID, "P", "CRMLineNotify", "", "")
        myConnDBStatus.exec_sp("""
        EXEC dbo.sp_UpdLineNotifyStts @Tran_ID = ?, @Send_Status = ?, @ModifyBy = ?, @Remarks = ?, @ErrorMsg = ?
        """, params)

        r = _lineNotify(token, msg, fileimage=row.File_Path)

        if r.status_code == 200:
            # Update Success Send_Status = 'S'
            params = (row.Tran_ID, "S", "CRMLineNotify", "", r.text)
            myConnDBStatus.exec_sp("""
            EXEC dbo.sp_UpdLineNotifyStts @Tran_ID = ?, @Send_Status = ?, @ModifyBy = ?, @Remarks = ?, @ErrorMsg = ?
            """, params)
            print("Tran_ID = {}, Msg => {}".format(row.Tran_ID, r.text))
        else:
            # Update Error Send_Status = 'E'
            params = (row.Tran_ID, "E", "CRMLineNotify", "", r.text)
            myConnDBStatus.exec_sp("""
            EXEC dbo.sp_UpdLineNotifyStts @Tran_ID = ?, @Send_Status = ?, @ModifyBy = ?, @Remarks = ?, @ErrorMsg = ?
            """, params)
            print("Tran_ID = {}, Msg => {}".format(row.Tran_ID, r.text))


if __name__ == '__main__':
    main()

