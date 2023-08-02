package io.github.ddsimoes.muddt

import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import oracle.jdbc.OracleArray
import oracle.jdbc.OracleConnection
import oracle.jdbc.OraclePreparedStatement
import oracle.jdbc.OracleTypes
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.Blob
import java.sql.Clob
import java.sql.Date
import java.sql.ParameterMetaData
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Struct
import java.sql.Time
import java.sql.Timestamp
import java.sql.Types

private const val NULL: Byte = 0
private const val NOTNULL: Byte = 1
private const val T_INT: Byte = 2
private const val T_LONG: Byte = 3
private const val T_STRING: Byte = 4
private const val T_DOUBLE: Byte = 5
private const val T_BIGDECIMAL: Byte = 6
private const val T_BLOB: Byte = 7
private const val T_CLOB: Byte = 7
private const val T_STRUCT: Byte = 32
private const val T_ARRAY: Byte = 33


sealed class Serializer {
    abstract fun serialize(output: Output, rs: ResultSet, index: Int)
    abstract fun deserialize(input: Input, ps: PreparedStatement, index: Int, parameterMetadata: ParameterMetaData)
}

abstract class ObjectSerializer<T> : Serializer() {
    abstract fun getObject(rs: ResultSet, index: Int): T?
    abstract fun writeObject(output: Output, value: T)
    abstract fun readObject(input: Input): T

    final override fun serialize(output: Output, rs: ResultSet, index: Int) {
        val b = getObject(rs, index)
        if (b == null) {
            output.writeByte(NULL)
        } else {
            output.writeByte(NOTNULL)
            writeObject(output, b)
        }
    }

    override fun deserialize(input: Input, ps: PreparedStatement, index: Int, parameterMetadata: ParameterMetaData) {
        val tag = input.readByte()
        if (tag == NULL) {
            ps.setObject(index, null)
        } else {
            check(tag == NOTNULL)
            ps.setObject(index, readObject(input))
        }
    }

}

object TimeSerializer : ObjectSerializer<Time>() {
    override fun getObject(rs: ResultSet, index: Int): Time? = rs.getTime(index)
    override fun writeObject(output: Output, value: Time) = output.writeLong(value.time)
    override fun readObject(input: Input): Time = Time(input.readLong())
}

object DateSerializer : ObjectSerializer<Date>() {
    override fun getObject(rs: ResultSet, index: Int): Date? = rs.getDate(index)
    override fun writeObject(output: Output, value: Date) = output.writeLong(value.time)
    override fun readObject(input: Input): Date = Date(input.readLong())
}

object TimestampSerializer : ObjectSerializer<Timestamp>() {
    override fun getObject(rs: ResultSet, index: Int): Timestamp? = rs.getTimestamp(index)
    override fun writeObject(output: Output, value: Timestamp) = output.writeLong(value.time)
    override fun readObject(input: Input): Timestamp = Timestamp(input.readLong())
}

object StringSerializer : ObjectSerializer<String>() {
    override fun getObject(rs: ResultSet, index: Int): String? = rs.getString(index)
    override fun writeObject(output: Output, value: String) = output.writeString(value)
    override fun readObject(input: Input): String = input.readString()
}

object IntegerSerializer : Serializer() {
    override fun serialize(output: Output, rs: ResultSet, index: Int) {
        val b = rs.getInt(index)
        if (b == 0 && rs.wasNull()) {
            output.writeByte(NULL)
        } else {
            output.writeByte(NOTNULL)
            output.writeVarInt(b, true)
        }
    }

    override fun deserialize(input: Input, ps: PreparedStatement, index: Int, parameterMetadata: ParameterMetaData) {
        val tag = input.readByte()
        if (tag == NULL) {
            ps.setObject(index, null)
        } else {
            check(tag == NOTNULL)
            ps.setInt(index, input.readVarInt(true))
        }        
    }

}

object BigDecimalSerializer : ObjectSerializer<BigDecimal>() {

    override fun getObject(rs: ResultSet, index: Int): BigDecimal? = rs.getBigDecimal(index)
    override fun writeObject(output: Output, value: BigDecimal) = write(output, value)
    override fun readObject(input: Input): BigDecimal = read(input)

    fun write(output: Output, value: BigDecimal) {
        // fast-path optimizations for BigDecimal constants
        if (value === BigDecimal.ZERO) {
            write(output, BigInteger.ZERO)
            output.writeInt(0, false) // for backwards compatibility
            return
        }
        // default behaviour
        write(output, value.unscaledValue())
        output.writeInt(value.scale(), false)
    }

    private fun write(output: Output, value: BigInteger) {
        // fast-path optimizations for BigInteger.ZERO constant
        if (value === BigInteger.ZERO) {
            output.writeByte(2)
            output.writeByte(0)
            return
        }
        // default behaviour
        val bytes = value.toByteArray()
        output.writeVarInt(bytes.size + 1, true)
        output.writeBytes(bytes)
    }

    fun read(input: Input): BigDecimal {
        val unscaledValue: BigInteger = readBigInteger(input)
        val scale = input.readInt(false)
        // fast-path optimizations for BigDecimal constants
        return if (unscaledValue === BigInteger.ZERO && scale == 0) {
            BigDecimal.ZERO
        } else BigDecimal(unscaledValue, scale)
    }

    private fun readBigInteger(input: Input): BigInteger {
        val length = input.readVarInt(true)
        val bytes = input.readBytes(length - 1)
        if (length == 2) {
            // Fast-path optimizations for BigInteger constants.
            when (bytes[0].toInt()) {
                0 -> return BigInteger.ZERO
                1 -> return BigInteger.ONE
                10 -> return BigInteger.TEN
            }
        }
        return BigInteger(bytes)
    }

}


object LongSerializer : Serializer() {
    override fun serialize(output: Output, rs: ResultSet, index: Int) {
        val b = rs.getLong(index)
        if (b == 0L && rs.wasNull()) {
            output.writeByte(NULL)
        } else {
            output.writeByte(NOTNULL)
            output.writeVarLong(b, true)
        }
    }

    override fun deserialize(input: Input, ps: PreparedStatement, index: Int, parameterMetadata: ParameterMetaData) {
        val tag = input.readByte()
        if (tag == NULL) {
            ps.setObject(index, null)
        } else {
            check(tag == NOTNULL)
            ps.setLong(index, input.readVarLong(true))
        }
    }

}

object DoubleSerializer : Serializer() {
    override fun serialize(output: Output, rs: ResultSet, index: Int) {
        val b = rs.getDouble(index)
        if (b == 0.0 && rs.wasNull()) {
            output.writeByte(NULL)
        } else {
            output.writeByte(NOTNULL)
            output.writeDouble(b)
        }
    }

    override fun deserialize(input: Input, ps: PreparedStatement, index: Int, parameterMetadata: ParameterMetaData) {
        val tag = input.readByte()
        if (tag == NULL) {
            ps.setObject(index, null)
        } else {
            check(tag == NOTNULL)
            ps.setDouble(index, input.readDouble())
        }
    }

}

object BlobSerializer : Serializer() {

    @Suppress("UsePropertyAccessSyntax")
    fun write(output: Output, value: Blob) {
        val length = value.length()
        check(length < Int.MAX_VALUE)
        val len = length.toInt()

        output.writeByte(T_BLOB)
        output.writeVarInt(len, true)
        val wLen = value.getBinaryStream().copyTo(output) // StreamUtils.streamCopy(value.getBinaryStream(), output)
        check(wLen == len.toLong())
    }

    override fun serialize(output: Output, rs: ResultSet, index: Int) {
        val value = rs.getBlob(index)
        if (value == null) {
            output.writeByte(NULL)
        } else {
            output.writeByte(NOTNULL)
            write(output, value)
        }
    }

    override fun deserialize(input: Input, ps: PreparedStatement, index: Int, parameterMetadata: ParameterMetaData) {
        val tag = input.readByte()
        if (tag == NULL) {
            ps.setObject(index, null)
        } else {
            check(tag == NOTNULL)
            ps.setBlob(index, input.inputStream)
        }
    }

}

object ClobSerializer : Serializer() {
    override fun serialize(output: Output, rs: ResultSet, index: Int) {
        val value = rs.getClob(index)

        if (value == null) {
            output.writeByte(NULL)
        } else {
            output.writeByte(NOTNULL)
            write(output, value)
        }
    }

    @Suppress("UsePropertyAccessSyntax")
    fun write(output: Output, value: Clob) {
        val length = value.length()
        check(length < Int.MAX_VALUE)
        output.writeByte(T_CLOB)
        output.writeVarInt(length.toInt(), true)
        val writer = output.writer()
        val wLen = value.getCharacterStream().copyTo(writer)
        writer.flush()
        check(wLen == length)
    }

    override fun deserialize(input: Input, ps: PreparedStatement, index: Int, parameterMetadata: ParameterMetaData) {
        TODO("Not yet implemented")
    }

}

object StructSerializer : ObjectSerializer<Struct>() {
    override fun getObject(rs: ResultSet, index: Int): Struct? = rs.getObject(index) as Struct?
    override fun writeObject(output: Output, value: Struct) = writeStruct(output, value)

    override fun deserialize(input: Input, ps: PreparedStatement, index: Int, parameterMetadata: ParameterMetaData) {
        val tag = input.readByte()
        if (tag == NULL) {
            if (ps is OraclePreparedStatement) {
                val typeName = parameterMetadata.getParameterTypeName(index)
                ps.setNull(index, OracleTypes.STRUCT, typeName)
            } else {
                ps.setObject(index, null, Types.STRUCT)
            }
        } else {
            check(tag == NOTNULL)
            check(input.readByte() == T_STRUCT)
            val struct = readStruct(input, ps)
            ps.setObject(index, struct)
        }
    }

    override fun readObject(input: Input): Struct = error("")

    private fun readStruct(input: Input, ps: PreparedStatement): Struct {
        val typeName = input.readString()
        val elements = readElements(input, ps)
        return ps.connection.createStruct(typeName, elements)
    }

    private fun readElements(input: Input, ps: PreparedStatement): Array<Any?> {
        val size = input.readVarInt(true)
        return Array(size) {
            read(input, ps)
        }
    }

    private fun read(input: Input, ps: PreparedStatement): Any? {
        val tag = input.readByte()

        if (tag == NULL) {
            return null
        }

        return when (tag) {
            T_INT -> {
                input.readVarInt(true)
            }
            T_LONG -> {
                input.readVarLong(true)
            }
            T_DOUBLE -> {
                input.readDouble()
            }
            T_STRING -> {
                input.readString()
            }
            T_ARRAY -> {
                readArray(input, ps)
            }
            T_STRUCT -> {
                readStruct(input, ps)
            }
            T_BIGDECIMAL -> {
                BigDecimalSerializer.read(input)
            }
            else -> error("Invalid type tag $tag")
        }
    }

    private fun readArray(input: Input, ps: PreparedStatement): java.sql.Array {
        val connection = ps.connection
        val arrayType = input.readString()
        val readElements = readElements(input, ps)
        if (connection is OracleConnection) {
            return connection.createARRAY(arrayType, readElements)
        }
        return connection.createArrayOf(arrayType, readElements)
    }

    private fun writeStruct(output: Output, value: Struct) {
        output.writeByte(T_STRUCT)
        output.writeString(value.sqlTypeName)
        val atts = value.attributes
        output.writeVarInt(atts.size, true)
        atts.forEach {
            write(output, it)
        }
    }

    fun write(output: Output, value: Any?) {
        if (value == null) {
            output.writeByte(NULL)
        } else {
            when (value) {
                is Int -> {
                    writeInt(output, value)
                }
                is Long -> {
                    writeLong(output, value)
                }
                is Double -> {
                    writeDouble(output, value)
                }
                is String -> {
                    writeString(output, value)
                }
                is java.sql.Array -> {
                    ArraySerializer.write(output, value)
                }
                is Struct -> {
                    writeStruct(output, value)
                }
                is BigDecimal -> {
                    writeBigDecimal(output, value)
                }
                is Blob -> {
                    BlobSerializer.write(output, value)
                }
                is Clob -> {
                    ClobSerializer.write(output, value)
                }
                else -> error("$value (${value::class}) not serializable.")
            }
        }
    }

    private fun writeInt(output: Output, value: Int) {
        output.writeByte(T_INT)
        output.writeVarInt(value, true)
    }

    private fun writeLong(output: Output, value: Long) {
        output.writeByte(T_LONG)
        output.writeVarLong(value, true)
    }

    private fun writeDouble(output: Output, value: Double) {
        output.writeByte(T_DOUBLE)
        output.writeDouble(value)
    }

    private fun writeString(output: Output, value: String) {
        output.writeByte(T_STRING)
        output.writeString(value)
    }

    private fun writeBigDecimal(output: Output, value: BigDecimal) {
        output.writeByte(T_BIGDECIMAL)
        BigDecimalSerializer.write(output, value)
    }

}

object ArraySerializer : Serializer() {
    fun write(output: Output, value: java.sql.Array) {
        output.writeByte(T_ARRAY)

        val baseTypeName = if (value is OracleArray) {
            value.oracleMetaData.name
        } else {
            value.baseTypeName
        }

        output.writeString(baseTypeName)

        val arrayOfAnys = value.array as Array<out Any?>
        output.writeVarInt(arrayOfAnys.size, true)
        arrayOfAnys.forEach {
            StructSerializer.write(output, it)
        }
    }

    override fun serialize(output: Output, rs: ResultSet, index: Int) {
        val value = rs.getArray(index)
        if (value == null) {
            output.writeByte(NULL)
        } else {
            output.writeByte(NOTNULL)
            write(output, value)
        }
    }

    override fun deserialize(input: Input, ps: PreparedStatement, index: Int, parameterMetadata: ParameterMetaData) {
        TODO("Not yet implemented")
    }

}

