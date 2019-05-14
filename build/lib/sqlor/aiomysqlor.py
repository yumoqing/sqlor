from .mysqlor import MySqlor

class AioMysqlor(MySqlor):
	@classmethod
	def isMe(self,name):
		return name=='aiomysql'

