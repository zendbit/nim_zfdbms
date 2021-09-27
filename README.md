### zfplugs is plugins for the zfcore framework
https://github.com/zendbit/nim.zfcore

#### install
```
nimble install zfdbms
```

#### direct install
```
https://github.com/zendbit/nim.zfdbms.git
```

#### usage
available plugins
```
##
##  database connection
##  database : mysql
##  database name: test
##  database user: admin
##  database password: 12345
##

import zfdbms/dbms

const DEFAULT_DATETIME_FORMAT* = "yyyy-MM-dd HH:mm:ss"
const DEFAULT_DATE_FORMAT* = "yyyy-MM-dd"

var dbi {.global.}: DBMS[MySql] = @[newDBMS[MySql]("sinacc")]

proc dbConn(connType: DbConnType): DBMS[MySql] {.gcsafe.} =
  # disable gcsafe checking
  {.cast(gcsafe).}:
    let conn = ord(connType)
    if dbi[conn].isNil or not dbi[conn].ping:
      echo "try connect to database: " &
        $dbi[conn].tryConnect

    result = dbi[conn]

proc createUID*(): string =
  let dtime = now().utc().format("yyyy-MM-dd HH:mm:ss:fffffffff")
  result = $($dtime).secureHash

proc sinaccDb*(): DBMS[MySql] {.gcsafe.} =
  result = dbConn(Sinacc)
```
