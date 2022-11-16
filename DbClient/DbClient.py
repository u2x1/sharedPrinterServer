# import sys
# sys.path.append("..")
# from threading import Lock
import pymysql
import queue
import json
from mytool.Order import Order
from mytool.File import File
# from Order import Order

class DbClient(object):
  def __init__(self, host, account, password, databaseName):
    self._host = host
    self._account = account
    self._password = password
    self._databaseName = databaseName
    self.conn_queue = queue.Queue(0)
    self.lastOrderID = self.readLastOrderId()[0][0] 

  def use_card(self, openid, file_name):
    self.write("doclog", "(openid,file_name,time)", f'("{openid}","{file_name}",NOW())')
    return self.read("doclog", "max(id)")[0][0]

  def update_position(self, position, file_id):
    self.update("doclog", "position", f"'{json.dumps(position)}'", "id", file_id)

  def read_card(self, openid, file_id, file_name):
    return self.read("doclog", "openid", id=file_id, file_name=f'"{file_name}"')

  def writeIP(self, ip:str):
    self.write("ip", "(ip, time)", f'("{ip}",NOW())')


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
      }
      return printer

  def activatePrinter(self, PrinterID):
    return self.update("myprinter", "status", 1, "printer_id", PrinterID)

  def deActivatePrinter(self, PrinterID):
    return self.update("myprinter", "status", 0, "printer_id", PrinterID)



  def change_page_direction(self, file_id, page_direction):
    return self.update("file", "page_direction", f"'{page_direction}'", "file_id", file_id)

  def addFile(self, file):
    if not file.page_direction:
      ret = self.write("file",
          (
            "(file_id, openid, order_id, file_name, storage_name, "
            "page_num, copy_num, is_duplex, page_range, page_direction, "
            "status, file_size, upload_time, file_type)"
          ),
          (
            f'({file.file_id}, "{file.openid}", {file.order_id}, '
            f'"{file.file_name}", "{file.storage_name}", {file.page_num or 0}, '
            f'{file.copy_num}, {file.is_duplex}, "{file.page_range}", '
            f'NULL, {file.status}, {file.file_size}, '
            f'NOW(), "{file.file_type}")'
          )
        )
    else:
      ret = self.write("file", 
        (
          "(file_id, openid, order_id, file_name, storage_name, "
          "page_num, copy_num, is_duplex, page_range, page_direction, "
          "status, file_size, upload_time)"
        ),
        (
          f'({file.file_id}, "{file.openid}", {file.order_id}, '
          f'"{file.file_name}", "{file.storage_name}", {file.page_num or 0}, '
          f'{file.copy_num}, {file.is_duplex}, "{file.page_range}", '
          f'"{file.page_direction}", {file.status}, {file.file_size}, '
          f'NOW())'
        )
        )
    if ret:
      return self.read("file", "max(file_id)")[0][0]

  def updateFileInfo(self, file):
    if not file.page_direction:
      sql = f'''
        update file set 
        is_duplex={file.is_duplex},
        copy_num={file.copy_num} 
        where file_id={file.file_id};
      '''
    else:
      sql = f'''
        update file set 
        is_duplex={file.is_duplex},
        page_direction='{file.page_direction}',
        copy_num={file.copy_num} 
        where file_id={file.file_id};
      '''
    ret = self.doSQL(sql, True)
    if ret:
      return True

  def addOrder(self, order):
    ret = self.write("myorder", 
      "(printer_id,openid,order_time,file_num,total_fee,is_pay,is_ack,status)", 
      f'({order.printer_id},"{order.openid}",NOW(),{order.file_num},{order.total_fee},{order.is_pay},{order.is_ack},{order.status})')
    if ret:
      # 返回新增订单的id
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
    return self.update("myorder", "storage_name", f"'{storageName}'", "order_id", orderID)

  def setPageNum(self, orderID, pageNum):
    return self.update("myorder", "page_num", pageNum, "order_id", orderID)

  def setPrintSide(self, orderID, printSide:str):
    return self.update("myorder", "print_side", f"'{printSide}'", "order_id", orderID)

  def submitorder(self, order_id, is_ack, file_num):
    sql = f'update myorder set is_ack={is_ack},file_num={file_num} where order_id={order_id};'
    return self.doSQL(sql, True)

  def ispay(self, order_id):
    sql = f'update myorder set is_pay=1 where order_id={order_id};'
    return self.doSQL(sql, True)

  def ack_printing(self, order_id):
    sql = f'update myorder set is_ack=1 where order_id={order_id};'
    return self.doSQL(sql, True)

  def update_order_fee(self, fee:int, order_id:int):
    return self.update("myorder", "total_fee", fee, "order_id", order_id)

  def dbresToOrderObject(self, ret):
    # 使用数据库查询结果生产order对象
    orderList = []
    for i in ret:
      oneOrder = Order(dbres=i)
      orderList.append(oneOrder)
    return orderList

  def dbres_to_file(self, ret):
    # 使用数据库查询结果生产file对象
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
    return self.update("file", "status", "True", "file_id", file_id)



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

  def write(self, table:str, keys:str, values:str):
    sql = f"INSERT INTO {table} {keys} VALUES {values};"
    return self.doSQL(sql, False)

  def update(self, table, key, value, column_name, column_value):
    sql = f"UPDATE {table} SET {key} = {value} WHERE {column_name} = {column_value}"
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
