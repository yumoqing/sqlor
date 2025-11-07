# python使用sqlor数据操作规范
## 安装依赖包
```
pip install apppublic
pip install sqlor
```

## 使用规范

### 包含所需的包
```
from sqlor.dbpools import DBPools
```

### 数据库操作

获得数据库连接
```
db = DBPools() 获得数据库连接
···

### 使用数据库操作上下文
使用数据库操作上下文的好处：
* 自动事务开始和结束，整体提交或整体失败
* 资源自动回收
```
async with db.sqlorContext(dbname) as sor:
	....
```
### 执行数据库操作
sqlor所有的数据库操作都必须在sqlorContext函数生成的上下文中执行，以提供事务一致性，并自动回收资源，提供两种模式的数据库操作，sqlExe函数可以执行dml， ddl，支持多表数据操作，而CRUD只针对单表提供增产改查
#### runSQL
写符合SQL标准的SQL语句，数据变量用“${varname}$"作为占位符，并在参数ns中给定参数

以下是一个查询用户数据的语句，机构id作为参数传入
```
sql = "select * from users where where orgid = ${myorgid}$ and delflg='0'"
await sor.sqlExe(sql, {'myorgid' : '12122'})
```
返回一个数据，数组中的每一个元素为一个用户数据

以下例子，返回分页数据，按照给定的page不同返回不同页的数据，pagerows定义每页数据量```
sql1 = "select * from stock"
dic = await sor.sqlPaging(sql, {'pagerows':50, 'page':1, 'sort':'stock_id'})
```
dic是个DictObject（从dict继承）有两个属性：total：查询结果总记录数， rows：page指定页的数据

#### CRUD
CRUD针对单表操作，提供对数据表的增删改查，C，D，U三个函数在ns参数中必须提供主键数据。
* C
向表中添加数据
```
await sor.C('stock', {
			'stock_id': '111111',
			...
		})
```
* R
从表中读取数据，支持分页字典数据返回和不分页数组数据返回
分页例子
```
ns = {'pagerows': 50, 'page':1, 'sort':stock_id'}
dic = await sor.R('stock', ns)
```
dic是个DictObject（从dict继承）有两个属性：total：查询结果总记录数， rows：page指定页的数据

不分页例子
```
ns = {'id':'666667'} # filters dictionary, find all records which id = '666667'
recs = await sor.R('stock', ns)
```

* U
修改数据表数据，以主键（必须在ns中出现）作为修改数据条件，修改ns中其他字段的值，ns中不出现的字段不会被修改
```
await sor.U('stock', {'stock_id':'111111', 'name':'new_name'})
```
* D
删除表中数据，以主键（必须在ns中出现）作为删除数据条件，从数据库中删除主键值与参数ns中主键值相同的记录
```
await sor.D('stock', {'stock_id':'111111'})
```
