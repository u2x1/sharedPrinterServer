
import json

# 合法的消息选项值
optionList = ["ok", "addOrder", "deleteOrder", "orderAcknowledge" "orderCompelete", "orderStatusUpdate", "orderError"]

class Message(object):
	def __init__(self, option, order=None, files=None):
		if option not in optionList:
			print("message create error!")
			return False
		self.option = option
		self.order = order
		if files:
			self.files = files

	def getMessage(self):
		return {
			"option": self.option,
			"order": self.order.getOrder() if self.order else None,
		}

	def getMessageString(self):
		return json.dumps(self.getMessage())

	def getMessageBytes(self):
		return bytes(self.getMessageString(),"utf-8")