package io.github.ddsimoes.muddt

import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import java.io.Closeable
import java.io.InputStream
import java.io.OutputStream
import java.sql.Connection
import java.sql.JDBCType
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types


private const val HEADER: Byte = 0
private const val RECORD: Byte = (0xFF).toByte()
private const val EOF: Byte = 3


class KryoWriter(
    outputStream: OutputStream,
    private val tableName: String,
    columns: List<String>,
    private val rs: ResultSet,
    private val limit: Int,
    private val printCount: Int
): Closeable {

    init {
        check(tableName.isNotBlank()) { "table name can't be blank" }
    }

    private val output = Output(outputStream)

    private val columns = rs.metaData.let { metadata ->
        Array(metadata.columnCount) {
            val index = it + 1
            Column(
                metadata.getColumnName(index),
                metadata.getColumnType(index),
                metadata.getPrecision(index),
                metadata.getScale(index)
            )
        }
    }

    fun run() {
        writeHeader()
        val columnCount = columns.size
        var count = 0L
        while ((limit < 0 || count < limit) && rs.next()) {
            count++
            output.writeByte(RECORD)
            for (i in 0 until columnCount) {
                try {
                    columns[i].serializer.serialize(output, rs, i + 1)
                } catch (e: Throwable) {
                    throw RuntimeException("Error writing column ${columns[i - 1].name} for record ${rs.row}.", e)
                }
            }
            if (count % printCount == 0L) {
                print("#rows: ")
                println(count)
            }
        }

        if (count % printCount != 0L) {
            print("#rows: ")
            println(count)
        }

    }

    private fun writeHeader() {
        output.writeByte(HEADER)
        output.writeString(tableName)
        output.writeVarInt(columns.size, true)
        columns.forEachIndexed { index, column ->
            print(index + 1)
            print(" - ")
            print(column.name)
            print(" (")
            print(column.sqlType)
            print("/")
            print(JDBCType.values().find { it.vendorTypeNumber == column.sqlType })
            println(")")
            output.writeString(column.name)
            output.writeVarInt(column.sqlType, true)
            output.writeVarInt(column.precision, true)
            output.writeVarInt(column.scale, true)
        }
    }

    override fun close() {
        output.writeByte(EOF)
        output.close()
    }

}


private data class Column(val name: String, val sqlType: Int, val precision: Int, val scale: Int) {

    val serializer: Serializer = findSerializer(sqlType)

    private fun findSerializer(sqlType: Int): Serializer {
        return when (sqlType) {
            Types.TIME, Types.TIME_WITH_TIMEZONE -> TimeSerializer
            Types.DATE -> DateSerializer
            Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> TimestampSerializer
            Types.CHAR, Types.VARCHAR,
            Types.NCHAR, Types.NVARCHAR,
            Types.LONGVARCHAR, Types.LONGNVARCHAR -> StringSerializer
            Types.DOUBLE, Types.FLOAT, Types.REAL -> DoubleSerializer
            Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.BIT, Types.BOOLEAN -> IntegerSerializer
            Types.BIGINT -> LongSerializer
            Types.DECIMAL, Types.NUMERIC -> {
                if (scale == 0) {
                    when {
                        precision < 10 -> {
                            IntegerSerializer
                        }
                        precision < 18 -> {
                            LongSerializer
                        }
                        else -> {
                            BigDecimalSerializer
                        }
                    }
                } else {
                    BigDecimalSerializer
                }
            }
            Types.BLOB -> BlobSerializer
            Types.CLOB, Types.NCLOB -> ClobSerializer
            Types.ARRAY -> ArraySerializer
            Types.STRUCT -> StructSerializer

            else -> error("not found")
        }
    }

}


class KryoReader(
    input: InputStream,
    private val jdbcManager: Connection,
    private val tableName: String,
    private val options: RawDumpLoader.Options
) : Runnable {

    private val input = Input(input)

    override fun run() {
        jdbcManager.createStatement().use { statement ->
            if (options.clear != ClearType.NO) {
                println("Delete $tableName")
                if (statement.executeUpdate("delete from $tableName") > 0) {
                    if (options.clear == ClearType.COMMIT) {
                        jdbcManager.commit()
                    }
                }
            }

            if (options.skipNonEmpty) {
                val nonEmpty = statement.executeQuery("select 1 from $tableName").use {
                    it.next()
                }
                if (nonEmpty) {
                    println("** Skipping non empty table $tableName **")
                    return
                }
            }
        }

        val columns = readHeader()

        val batchSize = options.batchSize

        val sql = "insert into $tableName (${columns.joinToString { it.name }}) values (${columns.joinToString { "?" }})"
        println(sql)

        println("Inserting data into $tableName")

        jdbcManager.prepareStatement(sql).use { ps ->
            readAndUpdate(ps, columns, batchSize, jdbcManager)
        }

    }

    @Suppress("ConvertTwoComparisonsToRangeCheck")
    private fun readAndUpdate(ps: PreparedStatement, columns: Array<Column>, batchSize: Int, connection: Connection) {
        val paramMetadata = ps.parameterMetaData

        var n = 0
        var batchCount = 0

        val limit = if (options.offset > 0 && options.limit >= 0) options.offset + options.limit else options.limit

        while (true) {
            val tag = input.readByte()
            if (tag == EOF || (limit > 0 && n >= limit)) {
                break
            }

            n++
            ps.clearParameters()
            for (i in columns.indices) {
                val column = columns[i]
                try {
                    column.serializer.deserialize(input, ps, i + 1, paramMetadata)
                } catch (e: Throwable) {
                    throw RuntimeException("Error reading column ${column.name} for record $n.", e)
                }
            }

            if (options.offset < 0 || n > options.offset) {
                ps.addBatch()
                batchCount++
            }

            if (batchCount % batchSize == 0) {
                try {
                    ps.executeBatch().forEach {
                        check(it > 0)
                    }
                    ps.clearBatch()
                    batchCount = 0
                } catch (e: Exception) {
                    println("Error (n=$n)")
                    throw e
                }
            }

            if (n % options.printCount == 0) {
                println(n)
            }

            if (options.commitCount > 0 && n % options.commitCount == 0) {
                println("Commit. ")
                connection.commit()
            }
        }
        if (batchCount % batchSize != 0) {
            ps.executeBatch().forEach {
                check(it > 0)
            }
            ps.clearBatch()
        }

        if (n % options.printCount != 0) {
            println(n)
        }

        if (options.commitCount == 0 || (options.commitCount > 0 && n % options.commitCount != 0)) {
            println("Commit. ")
            connection.commit()
        }
    }

    private fun readHeader(): Array<Column> {

        check(input.readByte() == HEADER)

        val tableName = input.readString()
        check(!tableName.isNullOrBlank())

        val nColumns = input.readVarInt( true)

        val columns = Array(nColumns) {
            Column(
                input.readString(),
                input.readVarInt(true),
                input.readVarInt(true),
                input.readVarInt(true))
        }

        columns.forEach {
            println(it)
        }

        return columns

    }
}