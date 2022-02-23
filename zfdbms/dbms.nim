##
##  zfdbms is dbms tools for generate, connect and query tools
##  This framework if free to use and to modify
##  License: BSD
##  Author: Amru Rosyada
##  Email: amru.rosyada@gmail.com
##  Git: https://github.com/zendbit/nim.zfdbms
##
##

import dbs
import std/tables

const FK_NOACTION* = "NO ACTION"
const FK_RESTRICT* = "RESTRICT"
const FK_CASCADE* = "CASCADE"
const FK_SETNULL* = "SET NULL"

when WITH_MYSQL:
  import db_mysql
when WITH_PGSQL:
  import db_postgres
when WITH_SQLITE:
  import db_sqlite

when WITH_MYSQL or WITH_PGSQL or WITH_SQLITE:
  import
    strformat,
    times,
    macros,
    tables,
    typetraits,
    strutils,
    sequtils,
    json,
    options,
    re

  export
    options,
    strutils,
    sequtils,
    json,
    strformat

  import
    stdext/[
      xjson,
      xstrutils,
      xsystem],
    dbs,
    dbssql

  export
    dbs,
    dbssql,
    xjson,
    xstrutils,
    xsystem

  type
    DbInfo* = tuple[
      database: string,
      username: string,
      password: string,
      host: string, port: int]

    DBMS*[T] = ref object
      connId*: string
      dbInfo*: DbInfo
      conn*: T
      connected*: bool
      dbmsStack*: Table[DbmsStmtType, DbmsFieldType]

    KVObj* = tuple[
      keys: seq[string],
      values: seq[string],
      nodesKind: seq[JsonNodeKind]]

    InsertIdResult* = tuple[
      ok: bool,
      insertId: int64,
      msg: string]

    UpdateResult* = tuple[
      ok: bool,
      affected: int64,
      msg: string]
    
    ExecResult* = tuple[
      ok: bool,
      msg: string]

    RowResult*[T] = tuple[
      ok: bool,
      row: T,
      msg: string]
    
    CountResult* = tuple[
      ok: bool,
      val: int64,
      msg: string]

    RowResults*[T] = tuple[
      ok: bool,
      rows: seq[T],
      msg: string]

    AffectedRowResults* = tuple[
      ok: bool,
      affected: int64,
      msg: string]

    DbmsDataType* = enum
      BIGINT
      INT
      SMALLINT
      DOUBLE
      FLOAT
      VARCHAR
      BOOL
      DATE
      TIME
      TIMESTAMP
      SERIAL
      TEXT

    DbmsType* = enum
      DBPGSQL
      DBMYSQL
      DBSQLITE

    DbmsStmtType* = enum
      SELECT
      MULTI_SELECT
      INSERT
      DELETE
      UPDATE
      INNERJOIN
      LEFTJOIN
      RIGHTJOIN
      FULLJOIN
      CREATE_TABLE
      COUNT

    DbmsFieldType* = ref object
      field*: JFieldPair
      isPrimaryKey*: bool
      isNull*: bool
      foreignKeyRef*: string
      name*: string
      isUnique*: bool
      useIndex*: bool
      length*: int64
      dataType*: DbmsDataType
      foreignKeyOnUpdate*: string
      foreignKeyOnDelete*: string
      foreignKeyFieldRef*: string
      tableName*: string
      timeFormat*: string
      dateFormat*: string
      timestampFormat*: string
      uniqueKeyName*: string
      indexName*: string

  ##
  ##  dbmsTable pragma this is for type definition
  ##  will map to database table name
  ##
  ##  example:
  ##
  ##  type
  ##    Users
  ##      {.dbmsTable("users").} = ref object
  ##      id
  ##        {.dbmsField(
  ##          isPrimaryKey = true,
  ##          dataType = SERIAL).}: Option[int]
  ##      name
  ##        {.dbmsField(
  ##          name = "full_name",
  ##          length = 255,
  ##          isNull = false).}: Option[string]
  ##      birthdate
  ##        {.dbmsField(
  ##          isNull = false,
  ##          dataType = TIMESTAMP).}: Option[string]
  ##      isOk {.ignoreField.}: Option[bool]
  ##      uid
  ##        {.dbmsField(
  ##          isNull = false,
  ##          isUnique = true,
  ##          length = 100).}: Option[string]
  ##  
  ##    Address
  ##      {.dbmsTable("address").} = ref object
  ##      id
  ##        {.dbmsField(isPrimaryKey = true,
  ##          dataType = SERIAL,
  ##          isNull = false).}: Option[int]
  ##      address
  ##        {.dbmsField(length = 255).}: Option[string]
  ##      usersId
  ##        {.dbmsField("users_id",
  ##          dataType = BIGINT,
  ##          isNull = false)
  ##          dbmsForeignKeyRef: Users
  ##          dbmsForeignKeyFieldRef: Users.id.}: Option[int]
  ##
  ##

  proc dbmsJsonFieldDesc(dbmsFieldTypes: JsonNode): seq[JFieldDesc] =
    for field in dbmsFieldTypes.to(seq[DbmsFieldType]):
      var name = field.name
      if name == "":
        name = field.field.name
      result.add((&"{field.tableName}.{name}", field.field.nodeKind))

  template dbmsTable*(name: string = "") {.pragma.}
  template dbmsField*(
    name: string = "",
    isNull: bool = true,
    length: int64 = 0,
    dataType: DbmsDataType = VARCHAR,
    timeFormat: string = "HH:mm:ss",
    dateFormat: string = "YYYY-MM-dd",
    timestampFormat: string = "YYYY-MM-dd HH:mm:ss") {.pragma.}
  template dbmsForeignKeyRef*(foreignKeyRef: typed) {.pragma.}
  template dbmsForeignKeyFieldRef*(foreignKeyFieldRef: typed) {.pragma.}
  template dbmsForeignKeyConstraint*(
    onDelete: string = FK_CASCADE,
    onUpdate: string = FK_CASCADE) {.pragma.}
  template dbmsCompositeUniqueKey*(keyName: string) {.pragma.}
  template dbmsUniqueKey*() {.pragma.}
  template dbmsCompositeIndex*(indexName: string) {.pragma.}
  template dbmsIndex*() {.pragma.}
  template dbmsPrimaryKey*() {.pragma.}

  proc newDBMS*[T](
    database: string,
    username: string,
    password: string,
    host: string,
    port: int): DBMS[T] {.gcsafe.} =
    ##
    ##  Create newDBMS for database connection with given parameter
    ##  let myConn = newDBMS[MySql](
    ##    "test", "admin",
    ##    "123321", "localhost",
    ##    3306)
    ##
    let c = newDbs[T](
      database,
      username,
      password,
      host,
      port).tryConnect()

    result = DBMS[T](
      connId: "",
      dbmsStack: initTable[DbmsStmtType, DbmsFieldType]())
    result.connected = c.success
    if c.success:
      result.conn = c.conn
    else:
      echo c.msg

  proc tryConnect*[T](self: DBMS[T]): bool {.gcsafe.} =
    ##
    ## Try connect to database
    ## Generic T is type of MySql, PgSql, SqLite
    ##

    let c = newDbs[T](
      self.dbInfo.database,
      self.dbInfo.username,
      self.dbInfo.password,
      self.dbInfo.host,
      self.dbInfo.port).tryConnect()
    self.conn = c.conn
    self.connected = c.success
    result = self.connected

  proc dbmsQuote*(str: string): string =
    ##
    ##  quote special char from the string make it valid for sql
    ##
    result = (fmt"{str}")
      .replace(fmt"\", fmt"\\")
      .replace(fmt"'", fmt"\'")
      .replace(fmt""" " """.strip, fmt""" \" """.strip)
      .replace(fmt"\x1a", fmt"\\Z")

  proc extractKeyValue*[T](
    self: DBMS,
    obj: T): KVObj {.gcsafe.} =
    ##
    ##  Extract key and value og the type
    ##  will discard null value
    ##  let kv = users.extractKeyValue
    ##
    ##  will retur KVObj
    ##
    ##  KVObj* = tuple[
    ##    keys: seq[string],
    ##    values: seq[string],
    ##    nodesKind: seq[JsonNodeKind]]
    ##
    var keys: seq[string] = @[]
    var values: seq[string] = @[]
    var nodesKind: seq[JsonNodeKind] = @[]
    let obj = %obj
    for k, v in obj.discardNull:
      if k.toLower.contains("-as-"): continue
      
      var skip = false
      for kf in obj.keys:
        if kf.toLower.endsWith(&"as-{k}"):
          skip = true
          break
      if skip: continue

      keys.add(k)
      nodesKind.add(v.kind)
      if v.kind != JString:
        values.add($v)
      else:
        values.add(v.getStr)

    result = (keys, values, nodesKind)

  proc dbmsQuote*(q: Sql): string =
    ##
    ## Quote Sql and return string, will quote special char to be valid sql
    ##
    let q = q.toQs
    var queries = q.query.split("?")
    for i in 0..q.params.high:
      let p = q.params[i]
      #var v = if p.nodeKind == JString and p.val != "null":
      #var v = if p.kind != JNull:
      #    #&"'{dbmsQuote(p.val)}'"
      #    &"'{dbmsQuote(v)}'"
      #  else:
      #    #p.val
      #    "null"
      var v =
        if p.kind == JString:
          if p.getStr != "null":
            &"'{dbmsQuote(p.getStr)}'"
          else:
            &"{dbmsQuote(p.getStr)}"
        elif p.kind == JNull:
          "null"
        else:
          $p

      queries.insert([v], (i*2) + 1)

    result = queries.join("")

  # insert into database
  proc insertId*[T](
    self: DBMS,
    table: string,
    obj: T): InsertIdResult {.gcsafe.} =
    ##
    ##  insert into database and return as InsertIdResult
    ##
    ##  let insert = db.insertId("users", Users(name: "Jhon"))
    ##  if insert.ok:
    ##    echo insert.insertId
    ##  echo insert.msg
    ##
    var q = Sql()
    try:
      if not self.connected:
        result = (false, 0'i64, "can't connect to the database.")
      else:
        let kv = self.extractKeyValue(obj)
        var fieldItems: seq[JsonNode] = @[]
        for i in 0..kv.keys.high:
          case kv.nodesKind[i]
          of JInt:
            fieldItems.add(%kv.values[i].tryParseBiggestUInt(0).val)
          of JFloat:
            fieldItems.add(%kv.values[i].tryParseBiggestFloat(0f).val)
          else:
            fieldItems.add(%kv.values[i])

        q = Sql()
          .insert(table, kv.keys)
          .value(fieldItems)
        
        result = (true,
          self.conn.insertId(sql dbmsQuote(q)),
          "ok")

    except Exception as ex:
      echo &"{ex.msg}, {q.toQs}"
      echo dbmsQuote(q)
      result = (false, 0'i64, ex.msg)

  proc update*[T](
    self: DBMS,
    table: string,
    obj: T,
    query: Sql): UpdateResult {.gcsafe.} =
    ##
    ##  update table will return UpdateResult
    ##
    ##  let update = db.update("users", Users(id: 100, name: "Jhon Doe"))
    ##  if update.ok:
    ##    echo update.affected
    ##  echo update.msg
    ##
    var q = Sql()
    try:
      if not self.connected:
        result = (false, 0'i64, "can't connect to the database.")
      else:
        let kv = self.extractKeyValue(obj)
        var fieldItems: seq[JsonNode] = @[]
        for i in 0..kv.keys.high:
          case kv.nodesKind[i]
          of JInt:
            fieldItems.add(%kv.values[i].tryParseBiggestUInt(0).val)
          of JFloat:
            fieldItems.add(%kv.values[i].tryParseBiggestFloat(0f).val)
          else:
            fieldItems.add(%kv.values[i])

        q = Sql()
          .update(table, kv.keys)
          .value(fieldItems) & query
        
        result = (true,
          self.conn.execAffectedRows(sql dbmsQuote(q)),
          "ok")

    except Exception as ex:
      echo &"{ex.msg}, {q.toQs}"
      echo dbmsQuote(q)
      result = (false, 0'i64, ex.msg)

  proc exec*(
    self: DBMS,
    query: Sql): ExecResult {.gcsafe.} =
    ##
    ##  execute the query will return ExecResult
    ##
    ##  db.exec(Sql().delete("users").where("users.id=?", %100))
    ##
    var q = Sql()
    try:
      if not self.connected:
        result = (false, "can't connect to the database.")
      else:
        q = query
        
        self.conn.exec(sql dbmsQuote(q))
        result = (true, "ok")

    except Exception as ex:
      echo &"{ex.msg}, {q.toQs}"
      echo dbmsQuote(q)
      result = (false, ex.msg)
      
  proc extractQueryResults*(fields: seq[JFieldDesc], queryResults: seq[string], fieldDelimiter: string = "."): JsonNode {.gcsafe.} =
    result = %*{}
    if queryResults.len > 0 and queryResults[0] != "" and queryResults.len == fields.len:
      for i in 0..fields.high:
        for k, v in fields[i].name.toDbType(fields[i].nodeKind, queryResults[i]):
          var fprops = k.split(" AS ")
          result[fprops[fprops.high].strip.replace(".", fieldDelimiter)] = v

  proc extractFieldsAlias*[T: JFieldDesc | JFieldPair](fields: seq[T]): seq[T] {.gcsafe.} =
    let fields = fields.map(proc (x: T): T =
      when T is JFieldDesc:
        (x.name.replace("-as-", " AS ").replace("-AS-", " AS "), x.nodeKind)
      else:
        (x.name.replace("-as-", " AS ").replace("-AS-", " AS "), x.val, x.nodeKind)
      )

  proc normalizeFieldsAlias*[T: JFieldDesc | JFieldPair](fields: seq[T]): seq[T] {.gcsafe.} =
    return fields.extractFieldsAlias.map(proc (x: T): T =
      when T is JFieldDesc:
        (x.name.split(" AS ")[0].strip, x.nodeKind)
      else:
        (x.name.split(" AS ")[0].strip, x.val, x.nodeKind)
      )
  
  proc getCount*(
    self: DBMS,
    query: Sql): CountResult {.gcsafe.} =
    ##
    ##  get count result return CountResult
    ##
    ##  let count = db.getCount(Sql().select("count(users.id)")
    ##    .fromTable("users").where("is_active=?", %true))
    ##  if count.ok:
    ##    echo count.count
    ##  echo msg
    ##
    try:
      if not self.connected:
        result = (false, 0.int64, "can't connect to the database.")
      else:
        let queryResults = self.conn.getRow(sql dbmsQuote(query))
        let countResult = tryParseBiggestInt(queryResults[0])
        result = (countResult.ok, countResult.val, "ok")
    except Exception as ex:
      echo &"{ex.msg}, {query.toQs}"
      echo dbmsQuote(query)
      result = (false, 0.int64, ex.msg)

  proc getRow*[T](
    self: DBMS,
    obj: T,
    query: Sql,
    fieldDelimiter: string = "."): RowResult[T] {.gcsafe.} =
    ##
    ##  get row from database will return RowResult
    ##
    ##  let r = db.getRow(Users(), Sql().where("users.id=?", %100))
    ##  if r.ok:
    ##    echo %r.row
    ##  echo r.msg
    ##
    try:
      if not self.connected:
        result = (false, obj, "can't connect to the database.")
      else:
        var fields: seq[JFieldDesc]
        when obj is JsonNode:
          fields = obj.dbmsJsonFieldDesc
        else:
          fields = obj.fieldDesc
        let queryResults = self.conn.getRow(sql dbmsQuote(query))
        result = (true, extractQueryResults(fields, queryResults, fieldDelimiter).to(T), "ok")
    except Exception as ex:
      echo &"{ex.msg}, {query.toQs}"
      echo dbmsQuote(query)
      result = (false, obj, ex.msg)

  proc getRow*[T](
    self: DBMS,
    table: string,
    obj: T,
    query: Sql,
    fieldDelimiter: string = "."): RowResult[T] {.gcsafe.} =
    ##
    ##  get row result from database and return RowResult
    ##
    ##  let r = db.getRow("users", Users(), Sql().where("users.id=?", %100))
    ##  if r.ok:
    ##    echo %r.row
    ##  echo r.msg
    ##
    var q = Sql()
    try:
      if not self.connected:
        result = (false, obj, "can't connect to the database.")
      else:
        var fields: seq[JFieldDesc]
        when obj is JsonNode:
          fields = obj.dbmsJsonFieldDesc
        else:
          fields = obj.fieldDesc
        q = (Sql()
          .select(fields.map(proc(x: JFieldDesc): string = x.name))
          .fromTable(table) & query)
         
        let queryResults = self.conn.getRow(sql dbmsQuote(q))
        result = (true, extractQueryResults(fields, queryResults, fieldDelimiter).to(T), "ok")
    except Exception as ex:
      echo &"{ex.msg}, {q.toQs}"
      echo dbmsQuote(q)
      result = (false, obj, ex.msg)

  proc getRows*[T](
    self: DBMS,
    obj: T,
    query: Sql,
    fieldDelimiter: string = "."): RowResults[T] {.gcsafe.} =
    ##
    ##  get multiple rows from database will return RowResults
    ##
    ##  let r = db.getRows(Users(), Sql().where("users.is_active=?", %true))
    ##  if r.ok:
    ##    echo %r.rows
    ##  echo r.msg
    ##
    try:
      if not self.connected:
        result = (false, @[], "can't connect to the database.")
      
      else:
        var fields: seq[JFieldDesc]
        when obj is JsonNode:
          fields = obj.dbmsJsonFieldDesc
        else:
          fields = obj.fieldDesc
        
        let queryResults = self.conn.getAllRows(sql dbmsQuote(query))
        
        var res: seq[T] = @[]
        if queryResults.len > 0 and queryResults[0][0] != "":
          for qres in queryResults:
            res.add(extractQueryResults(fields, qres, fieldDelimiter).to(T))
        result = (true, res, "ok")
    except Exception as ex:
      echo &"{ex.msg}, {query.toQs}"
      echo dbmsQuote(query)
      result = (false, @[], ex.msg)

  proc getRows*[T](
    self: DBMS,
    table: string,
    obj: T,
    query: Sql,
    fieldDelimiter: string = "."): RowResults[T] {.gcsafe.} =
    ##
    ##  get multiple rows from database will return RowResults
    ##
    ##  let r = db.getRows("users", Users(), Sql().where("users.is_active=?", %true))
    ##  if r.ok:
    ##    echo %r.rows
    ##  echo r.msg
    ##
    var q = Sql()
    try:
      if not self.connected:
        result = (false, @[], "can't connect to the database.")
      else:
        let fields = obj.fieldDesc
        q = (Sql()
          .select(fields.map(proc(x: JFieldDesc): string = x.name))
          .fromTable(table) & query)

        let queryResults = self.conn.getAllRows(sql dbmsQuote(q))
        var res: seq[T] = @[]
        if queryResults.len > 0 and queryResults[0][0] != "":
          for qres in queryResults:
            res.add(extractQueryResults(fields, qres, fieldDelimiter).to(T))
        result = (true, res, "ok")
    except Exception as ex:
      echo &"{ex.msg}, {q.toQs}"
      echo dbmsQuote(q)
      result = (false, @[], ex.msg)

  proc execAffectedRows*(
    self: DBMS,
    query: Sql): AffectedRowResults {.gcsafe.} =
    ##
    ##  exec query and get affected row will return AffectedRowResults
    ##  let r = db.execAffectedRows(Sql().delete("users").where("users.id=?", %100))
    ##  if r.ok:
    ##    echo r.affected
    ##  echo r.msg
    ##
    var q = Sql()
    try:
      if not self.connected:
        result = (false, 0'i64, "can't connect to the database.")
      else:
        q = query

        result = (true, self.conn.execAffectedRows(sql dbmsQuote(q)), "ok")
    except Exception as ex:
      echo &"{ex.msg}, {q.toQs}"
      echo dbmsQuote(q)
      result = (false, 0'i64, ex.msg)

  proc delete*[T](
    self: DBMS,
    table: string,
    obj: T,
    query: Sql): AffectedRowResults {.gcsafe.} =
    ##
    ##  exec delete query and get affected row will return AffectedRowResults
    ##  let r = db.delete("users", Users(id: 100))
    ##  if r.ok:
    ##    echo r.affected
    ##  echo r.msg
    ##
    var q = Sql()
    try:
      if not self.connected:
        result = (false, 0'i64, "can't connect to the database.")
      else:
        q = (Sql()
          .delete(table) & query)
        
        result = (true, self.conn.execAffectedRows(sql dbmsQuote(q)), "ok")
    except Exception as ex:
      echo &"{ex.msg}, {q.toQs}"
      echo dbmsQuote(q)
      result = (false, 0'i64, ex.msg)

  proc setEncoding(
    self: DBMS,
    encoding: string): bool {.gcsafe.} =
    ##
    ## sets the encoding of a database connection, returns true for success, false for failure
    ##
    if not self.connected:
      result = false
    else:
      result = self.conn.setEncoding(encoding)

  proc getDbInfo*(self: DBMS): DbInfo {.gcsafe.} =
    ##
    ##  get database info
    ##
    result = self.dbInfo

  # close the database connection
  proc close*(self: DBMS) {.gcsafe.} =
    ##
    ##  close database connection
    ##
    try:
      self.conn.close
    except:
      discard
    self.connected = false

  # test ping the server
  proc ping*(self: DBMS): bool {.gcsafe.} =
    ##
    ## ping to checn the database connection instance
    ## return true if connection active
    ##
    try:
      if not self.connected:
        result = self.connected
      else:
        discard self.conn.getRow(sql "SELECT 1")
        result = true
    except Exception as e:
      echo e.msg
      self.close
      discard

  # get connId
  proc connId*(self: DBMS): string {.gcsafe.} =
    ##
    ##  get database connId
    ##
    if not self.isNil:
      result = self.connId

  proc startTransaction*(self: DBMS): ExecResult {.gcsafe discardable.} =
    ##
    ##  start database transaction
    ##
    result = self.exec(Sql().startTransaction)

  proc commitTransaction*(self: DBMS): ExecResult {.gcsafe discardable.} =
    ##
    ##  commit database transaction
    ##
    result = self.exec(Sql().commitTransaction)

  proc savePointTransaction*(
    self: DBMS,
    savePoint: string): ExecResult {.gcsafe discardable.} =
    ##
    ##  create save point of transaction
    ##
    result = self.exec(Sql().savePointTransaction(savePoint))

  proc rollbackTransaction*(
    self: DBMS,
    savePoint: string = ""): ExecResult {.gcsafe discardable.} =
    ##
    ##  rollback transaction
    ##
    result = self.exec(Sql().rollbackTransaction(savePoint))
    if result.ok:
      result = self.exec(Sql().commitTransaction)

  proc toWhereQuery*[T](
    obj: T,
    tablePrefix: string = "",
    op: string = "AND"): tuple[where: string, params: seq[JsonNode]] =
    ##
    ##  convert object to qeuery syntanx default is AND operator
    ##
    var query: seq[string]
    var qParams: seq[JsonNode]
    let jObj = %obj
    for k, v in jObj:
      if v.kind in [JString, JInt, JFloat]:
        if tablePrefix != "":
          query.add(&"{tablePrefix}.{k}=?")
        else:
          query.add(&"{k}=?")

        case v.kind
        of JString:
          qParams.add(v)
        of JInt:
          qParams.add(v)
        of JFloat:
          qParams.add(v)
        else:
          discard

    result = (query.join(&" {op} ").strip, qParams)

  proc getDbType(dbms: DBMS): DbmsType =
    ##
    ##  get database type
    ##
    when WITH_PGSQL:
      result = DBPGSQL
    when WITH_MYSQL:
      result = DBMYSQL
    when WITH_SQLITE:
      result = DBSQLITE

  proc generateCreateTable(
    dbmsType: DbmsType,
    fieldList: seq[DbmsFieldType]): Sql =
    ##
    ##  create table syntax generator depend on database type
    ##
    var columns: seq[string] = @[]
    var primaryKey: seq[string] = @[]
    var foreignKey: seq[string] = @[]
    var tableName: string
    var uniqueKey: Table[string, seq[string]] = initTable[string, seq[string]]()
    var indexKey: Table[string, seq[string]] = initTable[string, seq[string]]()
    
    for f in fieldList:
      var column: seq[string] = @[]
      var columnName: string = ""

      if tableName == "":
        tableName = f.tableName

      if f.field.name != "":
        columnName = f.field.name
      else:
        columnName = f.name

      column.add(columnName)
      if f.isPrimaryKey:
        primaryKey.add(columnName)

      var isAutoInc = false
      case f.dataType
      of BIGINT:
        column.add("BIGINT")
      of INT:
        column.add("INT")
      of SMALLINT:
        column.add("SMALLINT")
      of DOUBLE:
        if dbmsType != DBPGSQL:
          column.add("DOUBLE")
        else:
          column.add("DOUBLE PRECISION")
      of FLOAT:
        if dbmsType != DBPGSQL:
          column.add("FLOAT")
        else:
          column.add("REAL")
      of VARCHAR:
        column.add("VARCHAR")
      of BOOL:
        column.add("BOOL")
      of TIME:
        column.add("TIME")
      of DATE:
        column.add("DATE")
      of TIMESTAMP:
        if dbmsType != DBPGSQL:
          column.add("DATETIME")
        else:
          column.add("TIMESTAMPTZ")
      of SERIAL:
        if dbmsType == DBMYSQL or dbmsType == DBSQLITE:
          # Mysql SqLite
          isAutoInc = true
          column.add("BIGINT")
        else:
          # PgSql SERIAL
          column.add("SERIAL")
      of TEXT:
        if dbmsType != DBPGSQL:
          column.add("LONGTEXT")
        else:
          column.add("TEXT")

      if f.length > 0:
        column.add(&"({f.length})")

      if dbmsType != DBPGSQL:
        if isAutoInc:
          if dbmsType == DBMYSQL:
            column.add("AUTO_INCREMENT")
          else:
            column.add("AUTOINCREMENT")

      if f.dataType != SERIAL and not f.isPrimaryKey:
        if f.isNull:
          column.add("NULL")
        else:
          column.add("NOT NULL")

        if f.isUnique:
          column.add("UNIQUE")

      if f.foreignKeyRef != "":
        var fkColRef = f.foreignKeyFieldRef
        if fkColRef == "":
          fkColRef = "id"

        var onUpdate = ""
        if f.foreignKeyOnUpdate != "":
          onUpdate = &"ON UPDATE {f.foreignKeyOnUpdate}"
        
        var onDelete = ""
        if f.foreignKeyOnDelete != "":
          onDelete = &"ON DELETE {f.foreignKeyOnDelete}"

        foreignKey.add(&"""FOREIGN KEY ({columnName}) REFERENCES {f.foreignKeyRef}({fkColRef}) {onUpdate} {onDelete}""")

        if f.useIndex:
          if not indexKey.hasKey(f.indexName):
            indexKey["default"] = @[]
          indexKey["default"].add(columnName)

      columns.add(column.join(" "))

      if f.uniqueKeyName != "":
        if not uniqueKey.hasKey(f.uniqueKeyName):
          uniqueKey[f.uniqueKeyName] = @[]
        uniqueKey[f.uniqueKeyName].add(columnName)
      
      if f.indexName != "":
        if not indexKey.hasKey(f.indexName):
          indexKey[f.indexName] = @[]
        indexKey[f.indexName].add(columnName)

    if primaryKey.len != 0:
      columns.add(&"""PRIMARY KEY({primaryKey.join(", ")})""")

    for k, v in uniqueKey:
      columns.add(&"""CONSTRAINT {k} UNIQUE ({v.join(", ")})""")
    
    for k, v in indexKey:
      if k != "default":
        columns.add(&"""INDEX {k} ({v.join(", ")})""")
      else:
        columns.add(&"""INDEX ({v.join(", ")})""")

    if foreignKey.len != 0:
      columns &= foreignKey
    
    result = Sql()
    result.stmt.add(&"""CREATE TABLE {tableName}({columns.join(", ")})""")

  proc generateSelectTable(
    fieldList: seq[DbmsFieldType],
    query: Sql = Sql(),
    withTablePrefix: bool = false): Sql =
    ##
    ##  select table syntax generator
    ##
    let q = Sql()
    var tableName = ""
    var fields: seq[string] = @[]
    var where = Sql()

    for f in fieldList:
      if tableName == "":
        tableName = f.tableName

      var fieldName = f.name
      if f.field.name != "":
        fieldName = f.field.name
      if withTablePrefix:
        fieldName = &"{f.tableName}.{fieldName}"
      fields.add(fieldName)
      
      if f.field.val != "null":
        if where.stmt.len == 0:
          discard where.where(&"{f.tableName}.{fieldName}=?", f.field.jValue)
        else:
          discard where.andWhere(&"{f.tableName}.{fieldName}=?", f.field.jValue)

    result = q.select(fields, not withTablePrefix).fromTable(tableName) & where & query

  proc generateCountTable(
    fieldList: seq[DbmsFieldType],
    query: Sql = Sql()): Sql =
    ##
    ##  count table syntax generator
    ##
    let q = Sql()
    var tableName = ""
    var where = Sql()

    for f in fieldList:
      if tableName == "":
        tableName = f.tableName

      var fieldName = f.name
      if f.field.name != "":
        fieldName = f.field.name
      
      if f.field.val != "null":
        if where.stmt.len == 0:
          discard where.where(&"{tableName}.{fieldName}=?", f.field.jValue)
        else:
          discard where.andWhere(&"{tableName}.{fieldName}=?", f.field.jValue)

    result = q.select("COUNT(*)", withTablePrefix = false).fromTable(tableName) & where & query

  proc generateInsertTable(
    multiFieldList: seq[seq[DbmsFieldType]]): Sql =
    ##
    ##  insert table syntax generator
    ##
    let q = Sql()
    # prepare of multiple insert
    # if single insert then get the first index
    #var multiValues: seq[seq[JFieldItem]] = @[]
    var multiValues: seq[seq[JsonNode]] = @[]
    var fields: seq[string] = @[]
    var tableName = ""
    var isExtractFieldComplete = false

    for multiField in multiFieldList:
      #var values: seq[JFieldItem] = @[]
      var values: seq[JsonNode] = @[]

      for f in multiField:
        # only set table name if empty
        if tableName == "":
          tableName = f.tableName

        var fieldName = f.name
        if f.field.name != "":
          fieldName = f.field.name

        if f.field.val != "null":
          if not isExtractFieldComplete:
            fields.add(fieldName)

          values.add(f.field.jValue)

      isExtractFieldComplete = true
      multiValues.add(values)

    if multiValues.len == 1:
      result = q.insert(tableName, fields).value(multiValues[0])
    else:
      result = q.insert(tableName, fields).values(multiValues)

  proc generateUpdateTable(
    fieldList: seq[DbmsFieldType],
    query: Sql = Sql()): Sql =
    ##
    ##  update table syntax generator
    ##
    let q = Sql()
    var tableName = ""
    #var value: seq[JFieldItem] = @[]
    var value: seq[JsonNode] = @[]
    let where = Sql()
    var fields: seq[string] = @[]

    for f in fieldList:
      if tableName == "":
        tableName = f.tableName

      var fieldName = f.name
      if f.field.name != "":
        fieldName = f.field.name
      fields.add(fieldName)
          
      value.add(f.field.jValue)
      
      if f.isPrimaryKey:
        if where.stmt.len == 0:
          discard where.where(&"{tableName}.{fieldName}=?", f.field.jValue)
        else:
          discard where.andWhere(&"{tableName}.{fieldName}=?", f.field.jValue)

    result = q.update(tableName, fields).value(value) & where & query

  proc generateDeleteTable(
    multiFieldList: seq[seq[DbmsFieldType]],
    query: Sql = Sql()): Sql =
    ##
    ##  delete table syntax generator
    ##
    let q = Sql()
    var tableName = ""
    let where = Sql()

    for multiField in multiFieldList:
      let fieldFilter = Sql()
      for f in multiField:
        if tableName == "":
          tableName = f.tableName

        var fieldName = f.name
        if f.field.name != "":
          fieldName = f.field.name

        if f.field.val != "null":
          if fieldFilter.stmt.len == 0:
            discard fieldFilter.append(&"{tableName}.{fieldName}=?", f.field.jValue)
          else:
            discard fieldFilter.append(&"AND {tableName}.{fieldName}=?", f.field.jValue)

      if where.stmt.len == 0:
        discard where.bracket(fieldFilter)
      else:
        discard where.append("OR").bracket(fieldFilter)

    result = q.delete(tableName).where(where) & query
    result.stmt = @[result.stmt.join(" ").replace("WHERE (())", " ")]

  proc generateJoinTable(
    fieldListTbl1: seq[DbmsFieldType],
    fieldListTbl2: seq[DbmsFieldType],
    stmtType: DbmsStmtType): Sql =
    ##
    ##  join table syntax generator
    ##
    let tableName: array[2, string] = [
      fieldListTbl1[0].tableName,
      fieldListTbl2[0].tableName]
    var fields: seq[string] = @[]
    var joinPair: seq[string] = @[]
    var joinType = ""
    let q = Sql()
   
    # get first tablename
    for f in fieldListTbl1:
      var fieldName = f.name
      if f.field.name != "":
        fieldName = f.field.name
      fields.add(&"{f.tableName}.{fieldName}")

      if f.foreignKeyFieldRef != "" and
        f.foreignKeyRef in tableName:
        joinPair.add(&"{f.tableName}.{fieldName}={f.foreignKeyRef}.{f.foreignKeyFieldRef}")

    # get second tablename
    for f in fieldListTbl2:
      var fieldName = f.name
      if f.field.name != "":
        fieldName = f.field.name
      fields.add(&"{f.tableName}.{fieldName}")

      if f.foreignKeyFieldRef != "" and
        f.foreignKeyRef in tableName:
        joinPair.add(&"{f.tableName}.{fieldName}={f.foreignKeyRef}.{f.foreignKeyFieldRef}")
      

    #discard q.select(fields, false).fromTable(tableName[0])
    
    case stmtType
    of INNERJOIN:
      discard q.innerJoin(tableName[1], joinPair)
    of LEFTJOIN:
      discard q.leftJoin(tableName[1], joinPair)
    of RIGHTJOIN:
      discard q.rightJoin(tableName[1], joinPair)
    of FULLJOIN:
      discard q.fullJoin(tableName[1], joinPair)
    else:
      discard

    result = q

  proc validatePragma[T](t: T): seq[DbmsFieldType] =
    ##
    ##  validation check pragma syntax, check pragma definition of the type
    ##
    when t.hasCustomPragma(dbmsTable):
      var fieldList: seq[DbmsFieldType] = @[]
      var dbmsTablePragma = t.getCustomPragmaVal(dbmsTable)
      if dbmsTablePragma == "":
        dbmsTablePragma = ($typeof(t)).split(":")[0]
      if dbmsTablePragma.strip != "":
        for k, v in system.fieldPairs(t):
          when v.hasCustomPragma(dbmsField):
            let dbmsFieldType = DbmsFieldType()
            dbmsFieldType.name = k
            dbmsFieldType.tableName = dbmsTablePragma
            
            let dbmsFieldPragma = v.getCustomPragmaVal(dbmsField)
            dbmsFieldType.isNull = dbmsFieldPragma.isNull
            dbmsFieldType.length = dbmsFieldPragma.length.int64
            dbmsFieldType.dataType = dbmsFieldPragma.dataType
            dbmsFieldType.timeFormat = dbmsFieldPragma.timeFormat
            dbmsFieldType.dateFormat = dbmsFieldPragma.dateFormat
            dbmsFieldType.timestampFormat = dbmsFieldPragma.timestampFormat

            var (name, val, nodeKind) = dbmsFieldPragma.name.fieldPair(v)
            if val != "null":
              if dbmsFieldType.dataType in [TIMESTAMP, TIME, DATE]:
                if val.contains("T"):
                  let dt = val.parse("yyyy-MM-dd'T'HH:mm:sszzz")

                  case dbmsFieldType.dataType
                  of TIMESTAMP:
                    val = dt.format(dbmsFieldType.timestampFormat)
                  of TIME:
                    val = dt.format(dbmsFieldType.timeFormat)
                  of DATE:
                    val = dt.format(dbmsFieldType.dateFormat)
                  else:
                    discard

            dbmsFieldType.field = (name, val, nodeKind)

            when v.hasCustomPragma(dbmsPrimaryKey):
              dbmsFieldType.isPrimaryKey = true
            
            when v.hasCustomPragma(dbmsUniqueKey):
              dbmsFieldType.isUnique = true

            when v.hasCustomPragma(dbmsCompositeUniqueKey):
              dbmsFieldType.uniqueKeyName = v.getCustomPragmaVal(dbmsCompositeUniqueKey)
            
            when v.hasCustomPragma(dbmsIndex):
              dbmsFieldType.useIndex = true

            when v.hasCustomPragma(dbmsCompositeIndex):
              dbmsFieldType.indexName = v.getCustomPragmaVal(dbmsCompositeIndex)

            when v.hasCustomPragma(dbmsForeignKeyRef):
              dbmsFieldType.foreignKeyRef = v.getCustomPragmaVal(dbmsForeignKeyRef).getCustomPragmaVal(dbmsTable)
              if dbmsFieldType.foreignKeyRef == "":
                dbmsFieldType.foreignKeyRef = ($(typeof v.getCustomPragmaVal(dbmsForeignKeyRef))).split(":")[0]
            
            when v.hasCustomPragma(dbmsForeignKeyFieldRef):
              dbmsFieldType.foreignKeyFieldRef = ($>v.getCustomPragmaVal(dbmsForeignKeyFieldRef)).replace(re"^(.+?)\.", "")
            
            when v.hasCustomPragma(dbmsForeignKeyConstraint):
              let pragmaConstraint = v.getCustomPragmaVal(dbmsForeignKeyConstraint)
              dbmsFieldType.foreignKeyOnUpdate = pragmaConstraint.onUpdate
              dbmsFieldType.foreignKeyOnDelete = pragmaConstraint.onDelete

            fieldList.add(dbmsFieldType)

      result = fieldList
    
    else:
      raise newException(ObjectConversionDefect, "object definition not contain pragma {.dbmsTable(table_name).}.")

  proc stmtTranslator[T1, T2](
    obj1: typedesc[T1],
    obj2: typedesc[T2],
    stmtType: DbmsStmtType): Sql =
    ##
    ## join statement generator
    ##
    let tbl1 = obj1()
    let tbl2 = obj2()
    var fieldListTbl1: seq[DbmsFieldType] = @[]
    var fieldListTbl2: seq[DbmsFieldType] = @[]

    when tbl1 is object:
      fieldListTbl1 = tbl1.validatePragma
    else:
      fieldListTbl1 = tbl1[].validatePragma

    when tbl2 is object:
      fieldListTbl2 = tbl2.validatePragma
    else:
      fieldListTbl2 = tbl2[].validatePragma

    if fieldListTbl1.len != 0 and fieldListTbl2.len != 0:
      result = generateJoinTable(fieldListTbl1, fieldListTbl2, stmtType)

  proc stmtTranslator[T](
    dbmsType: DbmsType,
    t: T,
    stmtType: DbmsStmtType,
    query: Sql = Sql()): Sql =
    ##
    ##  sql sstatement translator
    ##
    var multiFieldList: seq[seq[DbmsFieldType]] = @[]
    when t is JsonNode:
      multiFieldList.add(t.to(seq[DbmsFieldType]))
    else:
      when t is seq or t is array:
        # for multiple fieldlist, like insert, update, join
        for i in t:
          when i is object:
            multiFieldList.add(i.validatePragma)
          elif i is ref object:
            multiFieldList.add(i[].validatePragma)
      else:
        when t is object:
          multiFieldList.add(t.validatePragma)
        elif t is ref object:
          multiFieldList.add(t[].validatePragma)

    if multiFieldList.len != 0:
      case stmtType
      of SELECT:
        result = generateSelectTable(multiFieldList[0], query)
      of MULTI_SELECT:
        result = generateSelectTable(multiFieldList[0], query, true)
      of INSERT:
        result = generateInsertTable(multiFieldList)
      of UPDATE:
        result = generateUpdateTable(multiFieldList[0], query)
      of DELETE:
        result = generateDeleteTable(multiFieldList, query)
      of CREATE_TABLE:
        result = dbmsType.generateCreateTable(multiFieldList[0])
      of COUNT:
        result = generateCountTable(multiFieldList[0])
      else:
        discard

  proc createTable*[T](
    dbms: DBMS,
    t: T): ExecResult  =
    ##
    ##  create new table with given object
    ##
    ##  let ctbl = db.createTable(Users())
    ##  if ctbl.ok:
    ##    echo "table created"
    ##  echo ctbl.msg
    ##
    result = dbms.exec(
      dbms.getDbType.stmtTranslator(
        t,
        CREATE_TABLE))

  proc select*[T](
    dbms: DBMS,
    t: T,
    query: Sql = Sql()): RowResults[T] =
    ##
    ##  select multiple rows result from table with given object
    ##
    ##  let r = db.select(Users(isActive: some true))
    ##  if r.ok:
    ##    echo r.rows
    ##  echo r.msg
    ##
    if query.stmt.len == 0:
      discard query.limit(30)
    
    result = dbms.getRows(
      t,
      dbms.getDbType.stmtTranslator(
        t,
        SELECT,
        query))

  proc select*(
    dbms: DBMS,
    dbmsFieldTypes: seq[seq[DbmsFieldType]],
    fields: seq[string] = @[],
    query: Sql = Sql(),
    exceptFields: seq[string] = @[],
    fieldDelimiter: string = "."): RowResults[JsonNode] =
    ##
    ##  select multi row result from table with given objects,
    ##  the selectJoin is for join table
    ##
    ##  let r = dbConn(Sinacc)
    ##    .select(
    ##      [%@NetworkNas(), %@StatusType(), %@StateType()],
    ##      NetworkNas().innerJoin(StatusType())&
    ##      NetworkNas().innerJoin(StateType())&
    ##      NetworkNas().innerJoin(NetworkNasType()))
    ##  if r.ok:
    ##    echo r.rows
    ##  echo r.msg
    ##

    var dbmsField = dbmsFieldTypes.concat
    if fields.len > 0:
      dbmsField = dbmsField.filter(proc (x: DbmsFieldType): bool =
        var name = x.name
        if name == "":
          name = x.field.name
        result = &"{x.tableName}.{name}" in fields)
    result = dbms.getRows(
      %dbmsField,
      dbms.getDbType.stmtTranslator(
        %dbmsField,
        MULTI_SELECT,
        query),
      fieldDelimiter)

  proc selectOne*[T](
    dbms: DBMS,
    t: T,
    query: Sql = Sql()): RowResult[T] =
    ##
    ##  select single row result from table with given object
    ##
    ##  let r = db.selectOne(Users(id: some 100))
    ##  if r.ok:
    ##    echo r.row
    ##  echo r.msg
    ##
    result = dbms.getRow(
      t,
      dbms.getDbType.stmtTranslator(
        t,
        SELECT,
        query))

  proc selectOne*(
    dbms: DBMS,
    dbmsFieldTypes: seq[seq[DbmsFieldType]],
    fields: seq[string] = @[],
    query: Sql = Sql(),
    exceptFields: seq[string] = @[],
    fieldDelimiter: string = "."): RowResult[JsonNode] =
    ##
    ##  select single row result from table with given objects,
    ##  the selectJoin is for join table
    ##
    ##  let r = dbConn(Sinacc)
    ##    .selectOne(
    ##      [%@NetworkNas(), %@StatusType(), %@StateType()],
    ##      NetworkNas().innerJoin(StatusType())&
    ##      NetworkNas().innerJoin(StateType())&
    ##      NetworkNas().innerJoin(NetworkNasType()))
    ##  if r.ok:
    ##    echo r.row
    ##  echo r.msg
    ##

    var dbmsField = dbmsFieldTypes.concat
    if fields.len > 0:
      dbmsField = dbmsField.filter(proc (x: DbmsFieldType): bool =
        var name = x.name
        if name == "":
          name = x.field.name
        result = &"{x.tableName}.{name}" in fields)
    result = dbms.getRow(
      %dbmsField,
      dbms.getDbType.stmtTranslator(
        %dbmsField,
        MULTI_SELECT,
        query),
      fieldDelimiter)

  proc innerJoin*[T1, T2](
    tbl1: typedesc[T1],
    tbl2: typedesc[T2]): Sql =
    ##
    ##  inner join two object will return Sql object
    ##
    ##  let r = db.select(Users(isActive: some true),
    ##    Users().innerJoin(Address()) &
    ##    Address().leftJoin(DetailsAddress()))
    ##
    result = stmtTranslator(tbl1, tbl2, INNERJOIN)

  proc leftJoin*[T1, T2](
    tbl1: typedesc[T1],
    tbl2: typedesc[T2]): Sql =
    ##
    ##  inner join two object will return Sql object
    ##
    ##  let r = db.select(Users(isActive: some true),
    ##    Users().innerJoin(Address()) &
    ##    Address().leftJoin(DetailsAddress()))
    ##
    result = stmtTranslator(tbl1, tbl2, LEFTJOIN)

  proc rightJoin*[T1, T2](
    tbl1: typedesc[T1],
    tbl2: typedesc[T2]): Sql =
    ##
    ##  inner join two object will return Sql object
    ##
    ##  let r = db.select(Users(isActive: some true),
    ##    Users().innerJoin(Address()) &
    ##    Address().rightJoin(DetailsAddress()))
    ##
    result = stmtTranslator(tbl1, tbl2, RIGHTJOIN)

  proc count*[T](
    dbms: DBMS,
    t: T,
    query: Sql = Sql()): CountResult =
    ##
    ##  count row of given object
    ##
    ##  let count = db.count(Users(isActive: some true))
    ##  if count.ok:
    ##    echo count.count
    ##  echo count.msg
    ##
    result =  dbms.getCount(dbms.getDbType.stmtTranslator(t, COUNT, query))

  proc insert*[T](
    dbms: DBMS,
    t: T): AffectedRowResults =
    ##
    ##  insert to table with given object or list of object
    ##
    ##  let r = db.insert(Users(name: some "Jhon Doe"))
    ##  let mr = db.insert(
    ##    Users(name: some "Jhon Doe"),
    ##    Users(name: some "Michel Foe"))
    ##
    result = dbms.execAffectedRows(dbms.getDbType.stmtTranslator(t, INSERT))

  proc update*[T](
    dbms: DBMS,
    t: T,
    query: Sql = Sql()): AffectedRowResults =
    ##
    ##  update table with given object or list object
    ##
    ##  let r = db.update(Users(id: some 100, name: some "Jhon Chena"),
    ##    Users(id: some 200, name: some "Michel Bar"))
    ##
    var affectedRows: int64 = 0'i64
    var ok: bool
    var msg: string = "failed"

    when t is array or t is seq:
      for it in t:
        if dbms.execAffectedRows(dbms.getDbType.stmtTranslator(it, UPDATE, query)).ok:
          affectedRows += 1
          if msg == "":
            msg = "ok"
    else:
      if dbms.execAffectedRows(dbms.getDbType.stmtTranslator(t, UPDATE, query)).ok:
        affectedRows = 1
        msg = "ok"

    result = (ok, affectedRows, msg)

  proc delete*[T](
    dbms: DBMS,
    t: T,
    query: Sql = Sql()): AffectedRowResults =
    ##
    ##  delete from table with given obaject
    ##
    ##  let r = db.delete(Users(id: some 100))
    ##
    result = dbms.execAffectedRows(dbms.getDbType.stmtTranslator(t, DELETE, query))

  proc `%@`*[T](t: typedesc[T]): seq[DbmsFieldType] =
    let obj = t()
    when obj is object:
      result = obj.validatePragma()
    else:
      result = obj[].validatePragma()

  proc toDbmsTable*[T](
    jnode: JsonNode,
    t: typedesc[T],
    delimiter: string = "."): T =
    let node = newJObject()
    for k, v in jnode:
      let field = k.split(".")
      if field.len > 1:
        node[field[1]] = v
      else:
        node[k] = v

    result = node.to(t)
