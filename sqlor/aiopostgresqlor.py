
from .postgresqlor import PostgreSQLor
class AioPostgresqlor(PostgreSQLor):
	@classmethod
	def isMe(self,name):
		return name=='aiopg'


