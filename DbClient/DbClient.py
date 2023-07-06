# import sys
# sys.path.append("..")
# from threading import Lock
# from Order import Order
import pymysql
import queue
import json
from mytool.Order import Order
from mytool.File import File

def quote(s:str):
  return "'"+s+"'" if s is not None else 'NULL'

class DbClient(object):
  def __init__(self, host, account, password, databaseName):
    self._host = host
    self._account = account
    self._password = password
    self._databaseName = databaseName
    self.conn_queue = queue.Queue(0)
    self.lastOrderID = self.readLastOrderId()[0][0] 

  def use_card(self, openid, file_name):
    kvs = {
        'openid': quote(openid)
      , 'file_name': quote(file_name)
      , 'time': 'NOW()'
    }
    self.write("doclog", kvs)
    return self.read("doclog", "max(id)")[0][0]

  def update_position(self, position, file_id):
    self.update("doclog", "id", file_id,
      {"position": quote(json.dumps(position))})

  def read_card(self, openid, file_id, file_name):
    return self.read("doclog", "openid", id=file_id, file_name=f'"{file_name}"')

  def writeIP(self, ip:str):
    kvs = {
        'ip': quote(ip)
      , 'time': 'NOW()'
    }
    self.write("ip", kvs)


  def getPrinterList(self):
    results = self.read("myprinter", "*")
    printerList = []
    for i in results:
      printer = {
        "printer_id": i[0],
        "admin_id": i[1],
        "latitude": i[2],
        "longitude": i[3],
        "name": i[4],
        "status": i[5],
        "tips": i[6],
        "sleeping": i[7],
        "single_page_price": i[8],
        "duplex_price": i[9],
        "last_alive": i[10],
      }
      printerList.append(printer)
    return printerList

  def get_printer_info(self, printer_id):
    ret = self.read("myprinter", "*", printer_id=printer_id)
    for i in ret:
      printer = {
        "printer_id": i[0],
        "admin_id": i[1],
        "latitude": i[2],
        "longitude": i[3],
        "name": i[4],
        "status": i[5],
        "tips": i[6],
        "sleeping": i[7],
        "single_page_price": i[8],
        "duplex_price": i[9],
        "last_alive": i[10],
      }
      return printer

  def markPrinterSleep(self, id):
    return self.update("myprinter", "printer_id", id,
              {"sleeping": 1})

  def markPrinterAwake(self, id):
    return self.update("myprinter", "printer_id", id,
              {"sleeping": 0})
  
  def checkPrinterSleepness(self, id):
    return self.read("myprinter", "sleeping", printer_id=id)[0][0]

  def activatePrinter(self, id):
    return self.update("myprinter", "printer_id", id,
              {"status": 1})

  def deActivatePrinter(self, id):
    return self.update("myprinter", "printer_id", id,
              {"status": 0})


  def updateAliveTime(self, id):
    return self.update("myprinter", "printer_id", id,
              {"last_alive": "NOW()"})

  def addFile(self, file):
    kvs = {
        'file_id': file.file_id
      , 'file_size': file.file_size
      , 'file_type': quote(file.file_type)
      , 'file_name': quote(file.file_name)
      , 'storage_name': quote(file.storage_name)
      , 'openid': quote(file.openid)
      , 'order_id': file.order_id
      , 'page_num': file.page_num
      , 'copy_num': file.copy_num
      , 'is_duplex': file.is_duplex
      , 'is_booklet': file.is_booklet
      , 'crop_margin': file.crop_margin
      , 'page_range': quote(file.page_range)
      , 'page_direction': quote(file.page_direction)
      , 'status': file.status
      , 'upload_time': "NOW()"
    }
    if self.write("file", kvs):
      return self.read("file", "max(file_id)")[0][0]

  def updateFileInfo(self, file):
    return self.update("file", "file_id", file.file_id, {
          'is_duplex': file.is_duplex
        , 'is_booklet': file.is_booklet
        , 'crop_margin': file.crop_margin
        , 'page_direction': quote(file.page_direction)
        , 'copy_num': file.copy_num
      })
    

  def addOrder(self, order):
    kvs = {
        'printer_id': order.printer_id
      , 'openid': quote(order.openid)
      , 'order_time': 'NOW()'
      , 'file_num': order.file_num
      , 'total_fee': order.total_fee
      , 'is_pay': order.is_pay
      , 'is_ack': order.is_ack
      , 'status': order.status
    }
    if self.write("myorder", kvs):
      return self.read("myorder", "max(order_id)")[0][0]

  def get_user_orders(self, openid, start, count):
    sql = f'''
    select * from myorder where openid='{openid}' and file_num>0 order by order_time desc limit {start}, {count};
    '''
    ret = self.doSQL(sql, True)
    if ret:
      return self.dbresToOrderObject(ret)
    else:
      return []

  def setStorageName(self, orderID, storageName):
    return self.update("myorder", "order_id", orderID
              , {"storage_name": quote(storageName)})

  def setPageNum(self, orderID, pageNum):
    return self.update("myorder", "order_id", orderID
              , {"page_num": pageNum})

  def setPrintSide(self, orderID, printSide:str):
    return self.update("myorder", "order_id", orderID
              , {"print_side": quote(printSide)})

  def change_page_direction(self, file_id, page_direction):
    return self.update("file", "file_id", file_id, 
              {"page_direction": quote(page_direction)})

  def submitorder(self, order_id, is_ack, file_num):
    return self.update("myorder", "order_id", order_id, {
        "is_ack": is_ack
      , "file_num": file_num
      })

  def ispay(self, order_id):
    return self.update("myorder", "order_id", order_id, {
        "is_pay": 1
      })

  def ack_printing(self, order_id):
    return self.update("myorder", "order_id", order_id, {
        "is_ack": 1
      })

  def update_order_fee(self, fee:int, order_id:int):
    return self.update("myorder", "order_id", order_id
              , {"total_fee": fee})

  def dbresToOrderObject(self, ret):
    orderList = []
    for i in ret:
      oneOrder = Order(dbres=i)
      orderList.append(oneOrder)
    return orderList

  def dbres_to_file(self, ret):
    file_list = []
    for i in ret:
      file = File(dbres=i)
      file_list.append(file)
    return file_list

  def getOrderByPrinter(self, printer_id):
    ret = self.read("myorder", "*", printer_id=printer_id, status=0, is_ack=1, is_pay=1)
    return self.dbresToOrderObject(ret) if ret else []

  def get_files_by_order(self, order_id):
    ret = self.read("file", "*", order_id=order_id)
    return self.dbres_to_file(ret) if ret else []

  def getOrderById(self, orderID):
    ret = self.read("myorder", "*", order_id=orderID)
    return self.dbresToOrderObject(ret)[0] if ret else []

  def readLastOrderId(self):
    return self.read("myorder", "MAX(order_id)")

  def getUncompleteOrder(self):
    ret = self.read("myorder", "*", status=0)
    return self.dbresToOrderObject(ret)

  def fileComplete(self, file_id):
    return self.update("file", "file_id", file_id
              , {"status": True})



  def read(self, table, key, **condition):
    quireword = ""
    if condition:
      quire = []
      for i in condition:
        quire.append(f"{i} = {condition[i]}")
      string = " AND ".join(quire)
      quireword = f"WHERE {string}"
    sql = f"SELECT {key} FROM {table} {quireword};"
    return self.doSQL(sql, True)

  def write(self, table:str, kvs:dict):
    notFirst = False
    keys = ""
    values = ""
    for k,v in kvs.items():
      if notFirst:
        keys += ", "
        values += ", "
      else:
        notFirst = True

      keys += f"`{k}`"
      if v is not None:
        values += v if v is str else str(v)
      else:
        values += 'NULL'

    sql = f"INSERT INTO `{table}` ({keys}) VALUES ({values});"
    return self.doSQL(sql, False)

  def update(self, table:str, column_name:str, column_value, kvs:dict):
    notFirst = False
    sets = ""
    for k,v in kvs.items():
      if notFirst:
        sets += ", "
      else:
        notFirst = True
      
      sets += f"`{k}`={v if v is not None else 'NULL'}"
    sql = f"UPDATE `{table}` SET {sets} WHERE {column_name}={column_value}"
    return self.doSQL(sql, False)



  def doSQL(self, sql, needFetch):
    conn = self._get_conn()
    try:
      cur = conn.cursor()
      cur.execute(sql)
      if needFetch:
        rst = cur.fetchall()
      return rst if needFetch else True
    except Exception as e:
      conn.rollback()
      print(f"error on SQL[{sql}]: {e}")
      if not needFetch:
        return False
    finally:
      self._put_conn(conn)

  def _put_conn(self, conn):
    self.conn_queue.put(conn)

  def _get_conn(self):
    if not self.conn_queue.empty():
      conn = self.conn_queue.get()
      if conn: 
        return conn
    return self._create_new_conn()

  def _create_new_conn(self):
    return pymysql.connect(host=self._host
                         , user=self._account
                         , password=self._password
                         , database=self._databaseName, autocommit = True)



  def closeDB(self):
    try:
      while True:
        conn = self.conn_queue.get_nowait()
        if conn:
          conn.close()
    except queue.Empty:
        pass


if __name__ == '__main__':
  db = DbClient("localhost","root","58251190","printer")
  # db.closeDB()
