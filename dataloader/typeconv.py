convfuncs = {
	'int':int,
	'long':int,
	'llong':int,
	'float':float,
	'double':float,
	'date':str,
	'datetime':str,
	'ddouble':float
}

def conv(info, name, value):
	fields = info['fields']
	for f in fields:
		if f['name'] == name:
			f = convfuncs.get(f['type'], None)
			if f is None or value is None:
				return value
			return f(value)
	return value

def convrec(info, ns):
	ret = {k:conv(info, k, v) for k,v in ns.items()}
	return ret

