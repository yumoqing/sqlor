import re
from .sqlite3or import Sqlite3or

class Aiosqliteor(Sqlite3or):
	@classmethod
	def isMe(self,name):
		return name=='aiosqlite'
