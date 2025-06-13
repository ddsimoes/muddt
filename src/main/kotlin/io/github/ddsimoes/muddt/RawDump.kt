package io.github.ddsimoes.muddt

import oracle.jdbc.OracleStatement
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Struct
import java.util.*


class RawDump(val options: RawDumpOptions) : Runnable {

    data class RawDumpOptions(
        val properties: Map<String, String>,
        val type: String,
        val tablesFile: File,
        val dir: File,
        val fetchSize: Int,
        val printCount: Int,
        val limit: Int,
        val url: String
    )

    override fun run() {

        check(options.tablesFile.isFile) { "File ${options.tablesFile} not found." }

        check(options.dir.isDirectory) { "Directory ${options.dir} not found." }

        val jdbcManager = buildJdbcManager(options.url, options.properties)

        val errorTables = mutableListOf<String>()
        options.tablesFile.readLines().forEach { tableName ->
            if (tableName.isNotBlank()) {
                try {
                    dumpTable(jdbcManager, tableName)
                } catch (e: Exception) {
                    errorTables.add(tableName)
                    e.printStackTrace()
                }
            }
        }

        if (errorTables.isNotEmpty()) {
            val errorFile = options.dir / "error-${System.currentTimeMillis()}.txt"
            errorFile.printWriter().use { writer ->
                errorTables.forEach { table ->
                    writer.println(table)
                }
            }
        }

    }

    private fun dumpTable(jdbcManager: Connection, tableName: String) {
        printConnectionInfo(jdbcManager)

        println("Reading columns for $tableName")
        val columns = getColumns(jdbcManager, tableName)

        if (columns == null) {
            println("ERROR: table $tableName not found.")
            return
        }

        //, t.SHAPE.points as points
        val sql = "select ${columns.joinToString()} from $tableName t"

        val fetchSize = if (options.limit > 0) options.fetchSize.coerceAtMost(options.limit) else options.fetchSize
        val printCount = options.printCount

        if (options.type == "kryo") {
            jdbcManager.createStatement().use { stmt ->
                if (stmt is OracleStatement) {
                    stmt.lobPrefetchSize = 1024 * 1024
                }
                stmt.executeQuery(sql).use { rs ->
                    rs.fetchSize = fetchSize

                    KryoWriter(File(options.dir, "$tableName.kryo").outputStream(), rs, limit = options.limit, printCount = printCount).use {
                        it.run()
                    }
                }
            }
        }
    }

    private fun printConnectionInfo(jdbcManager: Connection) {
        val meta = jdbcManager.metaData

        println("DB connection info:")
        val productName = meta.databaseProductName
        val productVersion = meta.databaseProductVersion
        println("  name: $productName")
        println("  version: $productVersion")
    }

    private fun getColumns(
        jdbcManager: Connection,
        tableName: String
    ): List<String>? {

        val (schema, tblName) = if (tableName.indexOf('.') >= 0){
            tableName.split('.')
        } else {
            val schema = (options.properties["db.schema"] ?: options.properties["db.user"])?.uppercase(Locale.getDefault())
            listOf(schema, tableName)
        }

        return jdbcManager.metaData.getColumns(null, schema, tblName, null).toList {
            getString(4)
        }.takeIf { it.isNotEmpty() }
    }
}

private fun <T> ResultSet.toList(function: ResultSet.() -> T): List<T> {
    val list = mutableListOf<T>()
    this.use {
        while (it.next()) {
            list.add(it.function())
        }
    }
    return list
}

private operator fun File.div(s: String): File {
    return File(this, s)
}

class RawDumpLoader(private val options: Options) : Runnable {

    data class Options(
        val properties: Map<String, String>,

        val dir: File = File("."),

        val batchSize: Int = 1000,

        val printCount: Int = 10_000,

        val limit: Int = -1,

        val commitCount: Int = 0,

        val skipNonEmpty: Boolean = true,

        val clear: ClearType,

        val offset: Int = -1,

        val memInfo: Boolean = false,

        val url: String
    )

    override fun run() {

        //rollback
//        check(options.tablesFile.isFile) { "File ${options.tablesFile} not found." }

        check(options.dir.isDirectory) { "Directory ${options.dir} not found." }

        val jdbcManager = buildJdbcManager(options.url, options.properties)

        val files = options.dir.listFiles { file: File -> file.extension == "kryo" }

        check (files != null) { "No *.kryo files found at ${options.dir}" }

        val errorFile = options.dir / "error-${System.currentTimeMillis()}.txt"
        val runtime = Runtime.getRuntime()

        files.forEach { file ->

            val tableName = file.nameWithoutExtension

            if (tableName.isNotBlank()) {
                println(tableName)
                try {
                    file.inputStream().use { input ->
                        KryoReader(input, jdbcManager, tableName, options).run()
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                    errorFile.appendText("$tableName\n")
                }
                println("Memory in use: ${(runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024}mB")
            }
        }
    }

}

enum class ClearType {
    NO, YES, COMMIT
}

@Suppress("unused")
object StructTextConverter {

    private val sb = StringBuilder()

    fun invoke(value: Any?): Any {
        sb.setLength(0)
        append(value)
        return sb.toString()
    }

    private fun appendStruct(value: Struct) {
        sb.append("!")
        sb.append(value.sqlTypeName)
        sb.append(":")
        val attributes = value.attributes
        attributes.forEach {
            append(it)
            sb.append("|")
        }

        if (attributes.isNotEmpty()) {
            sb.setLength(sb.length - 1)
        }
    }

    private fun append(it: Any?) {
        when (it) {
            is java.sql.Array -> {
                appendArray(it)
            }

            is Struct -> {
                appendStruct(it)
            }

            null -> {
                //nothing
            }

            else -> {
                sb.append(it)
            }
        }
    }

    private fun appendArray(array: java.sql.Array) {
        val ao = array.array as Array<out Any?>
        sb.append("#")
        sb.append(ao.size)
        sb.append(":")
        ao.forEach {
            append(it)
            append("|")
        }
    }

}

private fun buildJdbcManager(url: String, properties: Map<String, String>): Connection {
    val dbProps = Properties()

    properties.forEach { (k, v) ->
        if (k.startsWith("db.")) {
            dbProps[k.removePrefix("db.")] = v
        }
    }

    return DriverManager.getConnection(url, dbProps).also {
        it.autoCommit = false
    }
}
