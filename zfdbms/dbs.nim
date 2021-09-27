##
##  zfcore web framework for nim language
##  This framework if free to use and to modify
##  License: BSD
##  Author: Amru Rosyada
##  Email: amru.rosyada@gmail.com
##  Git: https://github.com/zendbit/nim.zfplugs
##

const WITH_MYSQL* = defined(mysqldb) or defined(nimdoc)
const WITH_PGSQL* = defined(pgsqldb) or defined(nimdoc)
const WITH_SQLITE* = defined(sqlitedb) or defined(nimdoc)

when WITH_MYSQL:
  import db_mysql
  export db_mysql
when WITH_PGSQL:
  import db_postgres
  export db_postgres
when WITH_SQLITE:
  import db_sqlite
  export db_sqlite

when WITH_MYSQL or WITH_PGSQL or WITH_SQLITE:
  import strformat, json, strutils, strformat, options
  import stdext/[strutils_ext]

  when WITH_MYSQL:
    type
      MySql* = db_mysql.DbConn
  when WITH_PGSQL:
    type
      PgSql* = db_postgres.DbConn
  when WITH_SQLITE:
    type
      SqLite* = db_sqlite.DbConn
  
  type
    Dbs*[T] = ref object
      database: string
      username: string
      password: string
      host: string
      port: int

    DbsResult*[T] = tuple[
      success: bool,
      conn: T,
      msg: string]

  proc newDbs*[T](
    database: string,
    username: string = "",
    password: string = "",
    host: string = "",
    port: int = 0): Dbs[T] {.gcsafe.} =
    let instance = Dbs[T](
      database: database,
      username: username,
      password: password,
      host: host,
      port: port
    )

    result = instance

  proc tryConnect*[T](self: Dbs[T]): DbsResult[T] {.gcsafe.} =
    ##
    ## Try connect to database
    ## Generic T is type of MySql, PgSql, SqLite
    ##
    try:
      when WITH_PGSQL:
        if T is PgSql:
          result = (
            true,
            cast[T](db_postgres.open(
              &"{self.host}:{self.port}",
              self.username,
              self.password,
              self.database)),
            "OK")
      when WITH_MYSQL:
        if T is MySql:
          result = (
            true,
            cast[T](db_mysql.open(
              &"{self.host}:{self.port}",
              self.username,
              self.password,
              self.database)),
            "OK")
      when WITH_SQLITE:
        if T is SqLite:
          result = (
            true,
            cast[T](db_sqlite.open(
              self.database,
              "",
              "",
              "")),
            "OK")
      when not WITH_PGSQL and not WITH_MYSQL and not WITH_SQLITE:
        let dbType = $(type T)
        raise newException(ObjectConversionDefect, &"unknown database type {dbType}")
    except Exception as ex:
      result = (false, nil, ex.msg)

  proc tryCheckConnect*[T](self: Dbs[T]): DbsResult[T] {.gcsafe.} =
    try:
      var c = self.tryConnect[T]()
      if c.success:
        c.conn.get().close
      return (true, "OK")
    except Exception as ex:
      result = (false, ex.msg)

