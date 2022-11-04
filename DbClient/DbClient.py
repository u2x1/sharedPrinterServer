# import sys
# sys.path.append("..")
from threading import Lock
import pymysql
import time
import json
from mytool.Order import Order
from mytool.File import File
# from Order import Order

class DbClient(object):
	def __init__(self, host, account, password, databaseName):
		# 打开数据库连接
		self.db = pymysql.connect(host=host, user=account, password=password, database=databaseName, autocommit = True)
		# 使用cursor()方法获取操作游标 
		self.cursor = self.db.cursor()
		self.lastOrderID = self.readLastOrderId()[0][0] 
		self.lock = Lock()

	# 2022/5/22
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
		# 获得打印机列表
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
		res = self.read("myprinter", "*", printer_id=printer_id)
		for i in res:
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
		# 打印机启动
		return self.update("myprinter", "status", 1, "printer_id", PrinterID)
	def deActivatePrinter(self, PrinterID):
		# 打印机关闭
		return self.update("myprinter", "status", 0, "printer_id", PrinterID)

	def change_page_direction(self, file_id, page_direction):
		return self.update("file", "page_direction", f"'{page_direction}'", "file_id", file_id)

	def addFile(self, file):
		if not file.page_direction:
			res = self.write("file", 
				"(file_id, openid, order_id, file_name, storage_name, \
				page_num, copy_num, is_duplex, page_range, page_direction, \
				status, file_size, upload_time, file_type)"
				,
				f'({file.file_id}, "{file.openid}", {file.order_id}, \
				"{file.file_name}", "{file.storage_name}", {file.page_num or 0}, \
				{file.copy_num}, {file.is_duplex}, "{file.page_range}", \
				NULL, {file.status}, {file.file_size}, \
				NOW(), "{file.file_type}")'
				)
		else:
			res = self.write("file", 
				"(file_id, openid, order_id, file_name, storage_name, \
				page_num, copy_num, is_duplex, page_range, page_direction, \
				status, file_size, upload_time)"
				,
				f'({file.file_id}, "{file.openid}", {file.order_id}, \
				"{file.file_name}", "{file.storage_name}", {file.page_num or 0}, \
				{file.copy_num}, {file.is_duplex}, "{file.page_range}", \
				"{file.page_direction}", {file.status}, {file.file_size}, \
				NOW())'
				)
		if res:
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
		res = self.sql(sql)
		if res:
			return True

	def addOrder(self, order):
		# 向数据库添加一个订单
		# print(f'({order.printerID},"{order.orderUser}",NOW(),{order.pageNum},"{order.printType}","{file_list_write}","{file_size_write}","{str(order.isDuplex)}")')
		res = self.write("myorder", 
			"(printer_id,openid,order_time,file_num,total_fee,is_pay,is_ack,status)", 
			f'({order.printer_id},"{order.openid}",NOW(),{order.file_num},{order.total_fee},{order.is_pay},{order.is_ack},{order.status})')
		if res:
			# 返回新增订单的id
			return self.read("myorder", "max(order_id)")[0][0]

	def get_user_orders(self, openid, start, count):
		sql = f'''
		select * from myorder where openid='{openid}' and file_num>0 order by order_time desc limit {start}, {count};
		'''
		res = self.sql(sql)
		if res:
			return self.dbresToOrderObject(res)
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
		print(sql)
		return self.sql(sql)

	def ispay(self, order_id):
		sql = f'update myorder set is_pay=1 where order_id={order_id};'
		if self.sql(sql)==False:
			return False
		else:
			return True

	def ack_printing(self, order_id):
		sql = f'update myorder set is_ack=1 where order_id={order_id};'
		return self.sql(sql)

	def update_order_fee(self, fee:int, order_id:int):
		return self.update("myorder", "total_fee", fee, "order_id", order_id)

	def dbresToOrderObject(self, res):
		# 使用数据库查询结果生产order对象
		orderList = []
		for i in res:
			oneOrder = Order(dbres=i)
			orderList.append(oneOrder)
		return orderList

	def dbres_to_file(self, res):
		# 使用数据库查询结果生产file对象
		file_list = []
		for i in res:
			file = File(dbres=i)
			file_list.append(file)
		return file_list

	def getOrderByPrinter(self, printer_id):
		res = self.read("myorder", "*", printer_id=printer_id, status=0, is_ack=1, is_pay=1)
		if res:
			return self.dbresToOrderObject(res)
		else:
			return []

	def get_files_by_order(self, order_id):
		res = self.read("file", "*", order_id=order_id)
		if res:
			return self.dbres_to_file(res)
		else:
			return []

	def getOrderById(self, orderID):
		res = self.read("myorder", "*", order_id=orderID)
		if res:
			return self.dbresToOrderObject(res)[0]
		else: 
			return []

	def getUncompleteOrder(self):
		res = self.read("myorder", "*", status=0)
		return self.dbresToOrderObject(res)

	def fileComplete(self, file_id):
		return self.update("file", "status", "True", "file_id", file_id)

	def readLastOrderId(self):
		sql = "SELECT MAX(order_id) FROM myorder;"
		try:
			# 执行SQL语句
			self.cursor.execute(sql)
			# 获取所有记录列表
			results = self.cursor.fetchall()
			return results
		except Exception as e:
			print ("Error: unable to fecth data")
			print(e)

	def read(self, table, key, **condition):
		quireword = ""
		if condition:
			quire = []
			for i in condition:
				quire.append(f"{i} = {condition[i]}")
			string = " AND ".join(quire)
			quireword = f"WHERE {string}"
		# SQL 查询语句
		sql = f"SELECT {key} FROM {table} {quireword};"
		try:
			# 执行SQL语句
			self.lock.acquire()
			self.cursor.execute(sql)
			# 获取所有记录列表
			results = self.cursor.fetchall()
			self.lock.release()
			return results
		except Exception as e:
			self.cursor.nextset()
			if (self.lock.locked()): 
				self.lock.release()
			print ("Error: unable to fecth data")
			print("error:", e)
			print("sql:", sql)

	def write(self, table:str, keys:str, values:str):
		sql = f"INSERT INTO {table} {keys} VALUES {values};"
		try:
			# 执行sql语句
			self.lock.acquire()
			self.cursor.execute(sql)
			# 提交到数据库执行
			self.db.commit()
			self.lock.release()
			return True
		except Exception as e:
			# 如果发生错误则回滚
			self.db.rollback()
			if (self.lock.locked()): 
				self.lock.release()
			print("error:", e)
			print("sql:", sql)
			return False

	def update(self, table, key, value, column_name, column_value):
		# SQL 更新语句
		sql = f"UPDATE {table} SET {key} = {value} WHERE {column_name} = {column_value}"
		try:
			# 执行SQL语句
			self.lock.acquire()
			self.cursor.execute(sql)
			# 提交到数据库执行
			self.db.commit()
			self.lock.release()
			return True
		except Exception as e:
			# 发生错误时回滚
			self.db.rollback()
			if (self.lock.locked()): 
				self.lock.release()
			print("error:", e)
			print("sql:", sql)
			return False

	def sql(self, sql):
		try:
			# 执行SQL语句
			self.lock.acquire()
			self.cursor.execute(sql)
			# 提交到数据库执行
			results = self.cursor.fetchall()
			self.db.commit()
			self.lock.release()
			return results
		except Exception as e:
			# 发生错误时回滚
			self.cursor.nextset()
			self.db.rollback()
			if (self.lock.locked()): 
				self.lock.release()
			print("error:", e)
			print("sql:", sql)
			return False

	def closeDB(self):
	# 关闭数据库连接
		self.db.close()

if __name__ == '__main__':
	db = DbClient("localhost","root","58251190","printer")

	# db.closeDB()
