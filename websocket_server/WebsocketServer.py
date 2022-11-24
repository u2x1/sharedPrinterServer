
import time
import os
import threading
import json
import asyncio
import websockets
import struct
from queue import Queue
# from utils.Order import Order
# from utils.Message import Message
from Order import Order
from Message import Message
from DbClient import DbClient

class WsServer(object):
  def __init__(self, orderList, db):
    self.orderList = orderList
    self.db = db

  async def hello(self, websocket, path):
    print("server is waitting...")
    message = json.loads(await websocket.recv())
    self.db.activatePrinter(message["deviceID"])
    print(f"<<< {message}")
    await websocket.send(Message("ok").getMessageString())
    print("send ok")
    while True:
      time.sleep(0.1)
      if not self.orderList.empty():
        print("尝试获得订单")
        oneOrder = self.orderList.get() # 阻塞方法
        print("获得订单")
        for fileName in oneOrder.fileList:
          with open(f"orderFiles/{fileName}", "rb") as file:
            sendfile = file.read()
          oneMessage = Message("addOrder", oneOrder)
          print(oneMessage.getMessageString())
          await websocket.send(oneMessage.getMessageString())
          print("发不出去？")
          await websocket.send(struct.pack(">i", oneOrder.orderID)+sendfile)
          print("fabushuqu?")

  async def main(self):
    async with websockets.serve(self.hello, "localhost", 5001):
      await asyncio.Future()  # run forever

  def run(self):
    asyncio.run(self.main())

if __name__=="__main__":
  orderList = Queue(maxsize=0)
  db = DbClient("localhost","root","58251190","printer")
  wsServer = WsServer(orderList, db)
  wsServer.run()