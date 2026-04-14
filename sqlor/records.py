from appPublic.dictObject import DictObject

class Records:
	def __init__(self,klass=DictObject):
		self._records = []
		self.klass = klass

	def add(self,rec):
		obj = self.klass(**rec)
		self._records.append(obj)

	def get(self):
		return self._records


	def __iter__(self):
		self.start = 0
		self.end = len(self._records)
		return self

	def __next__(self):
		if self.start < self.end:
			d = self._records[self.start]
			self.start = self.start + 1
			return d
		else:
			raise StopIteration
