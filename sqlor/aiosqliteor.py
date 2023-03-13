import re
from .sqlite3or import SQLite3or

class Aiosqliteor(SQLite3or):
	@classmethod
	def isMe(self,name):
		return name=='aiosqlite'
