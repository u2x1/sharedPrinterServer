
from time import time, sleep
from threading import Thread


class AlivePrinters(object):
  def __init__(self, db):
    # 键：printerid，值：打印机上次活动的时间戳
    self.alivePrinters = {}
    self.db = db

  def add(self, PrinterID):
    self.db.activatePrinter(PrinterID)
    self.alivePrinters[PrinterID] = time()
    Thread(target=self.timeoutCount, args=[PrinterID]).start()
    # print("activate", PrinterID)

  def remove(self, PrinterID):
    del self.alivePrinters[PrinterID]
    self.db.deActivatePrinter(PrinterID)
    # print("deactivate", PrinterID)

  def timeoutCount(self, PrinterID):
    while True:
      # 打印机120秒没有活动则判定下线
      try:
        sleep(120)
        if time()-self.alivePrinters[PrinterID]<240:
          self.remove(PrinterID)
          break # 该打印机不再存活
      except Exception as e:
        print(e)
        break

  def keepAlive(self, PrinterID):
    # 每次打印机活动调用一次，更新最新活动时间
    if PrinterID not in self.alivePrinters:
      self.add(PrinterID)
    else:
      self.alivePrinters[PrinterID] = time()
      # print(PrinterID, "keepalive")