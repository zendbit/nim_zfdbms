### zfdbms simple dbms generator, connector and query tools
https://github.com/zendbit/nim.zfdbms

#### install
```
nimble install zfdbms
```

#### install latest
```
nimble install zfdbms@#head
```

#### direct install
```
nimble install https://github.com/zendbit/nim.zfdbms
```

#### important note
need to pass when compile -d:mysqldb or -d:pgsqldb or -d:sqlitedb depend on your database target. or can pass all parameter.
```
nim c -d:mysqldb myapp.nim
```

#### usage
```nim
# 
# console app example
# database engine: mysql
# database name: testdb
# database username: admin
# database password: helloworld
# database host: localhost
# database port: 3306
#
import zfdbms/dbms
import strformat
import json
##
## create database connection
##
var dbi {.global.}: DBMS[MySql] = newDBMS[MySql](
  "testdb",
  "admin",
  "helloworld",
  "localhost",
  3306)

##
## function check database connection when call it
## if connection doesn't exist or destroyed try to connect
## return DBMS type
##
proc dbConn*(): DBMS[MySql] {.gcsafe.} =
  # disable gcsafe checking
  {.cast(gcsafe).}:
    if dbi.isNil or not dbi.ping:
      echo "try connect to database: " &
        $dbi.tryConnect

    result = dbi

##
## test create database
## using the macro
## see file zfdbms/dbms.nim template section
##
type
  Users*
    {.dbmsTable.} = ref object
    id*
      {.dbmsField(
        dataType = SERIAL)
        dbmsPrimaryKey.}: Option[int64]
    email*
      {.dbmsField(
        isNull = false,
        dataType = VARCHAR,
        length = 100)
        dbmsUniqueKey.}: Option[string]
    name*
      {.dbmsField(
        isNull = false,
        dataType = VARCHAR,
        length = 100).}: Option[string]

  UsersDetails*
    {.dbmsTable.} = ref object
    id*
      {.dbmsField(
        dataType = SERIAL)
        dbmsPrimaryKey.}: Option[int64]
    usersId*
      {.dbmsField(
        isNull = false,
        dataType = BIGINT)
        dbmsForeignKeyRef: Users
        dbmsForeignKeyFieldRef: Users.id
        dbmsForeignKeyConstraint(
          onDelete = FK_CASCADE,
          onUpdate = FK_CASCADE).}: Option[int64]
    address*
      {.dbmsField(
        dataType = VARCHAR,
        length = 255).}: Option[string]


##
##  create table structure depend on the above type
##
when isMainModule:
  echo &"create table {$@Users} {dbConn().createTable(Users()).msg}"
  echo &"create table {$@UsersDetails} {dbConn().createTable(UsersDetails()).msg}"

  ##
  ##  insert users
  ##
  let newuser = dbConn().insert(
    @[Users(name: some "John Doe", email: some "jdoe@email.com"),
    Users(name: some "Jogn Foe", email: some "jfoe@email.com")])
  
  if newUser.ok:
    ##  get jdoe
    let jdoe = dbConn().selectOne(Users(email: some "jdoe@email.com"))
    if jdoe.ok:
      ##  add new users details
      if dbConn().insert(UsersDetails(usersId: jdoe.row.id, address: some "Yogyakarta")).ok:
        echo "jdoe address updated"

    ##  get jfoe
    let jfoe = dbConn().selectOne(Users(email: some "jfoe@email.com"))
    if jfoe.ok:
      ##  add new users details
      if dbConn().insert(UsersDetails(usersId: jfoe.row.id, address: some "Bantul")).ok:
        echo "jfoe address updated"

    ##  select users with details
    ##  select with join multiple table
    ##  return RowResults[JsonNode]
    echo "\n\nselect with join\n"
    let usersdata = dbConn()
      .select(
        @[%@Users, %@UsersDetails],
        @[$>Users.id, $>Users.name, $>Users.email, $>UsersDetails.address],
        Users.innerJoin(UsersDetails))

    for u in usersdata.rows:
      echo "---------------------"
      echo &"id: " & $u{$>Users.id}.getInt
      echo &"name: " & u{$>Users.name}.getStr
      echo &"email:" & u{$>Users.email}.getStr
      echo &"address:" & u{$>UsersDetails.address}.getStr

    ##  select users
    ##  select user with no join will return the table object
    ##  return RowResults[T]
    echo "\n\nselect single table multiple rows\n"
    let users = dbConn().select(Users())
    for u in users.rows:
      echo "---------------------"
      echo &"id: " & $u.id.get
      echo &"name: " & u.name.get
      echo &"email:" & u.email.get
    
    echo "\n\nselect single table single row\n"
    let user = dbConn().selectOne(Users(), Sql().where(&"{$@Users.email}=?", %"jdoe@email.com"))
    echo "---------------------"
    echo &"id: " & $user.row.id.get
    echo &"name: " & user.row.name.get
    echo &"email:" & user.row.email.get
```
