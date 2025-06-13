import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import io.github.ddsimoes.muddt.BigDecimalSerializer
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.math.BigDecimal
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SerializerTest {
    @Test
    fun `bigdecimal roundtrip`() {
        val outputStream = ByteArrayOutputStream()
        val output = Output(outputStream)
        BigDecimalSerializer.write(output, BigDecimal("123.45"))
        output.close()
        val input = Input(ByteArrayInputStream(outputStream.toByteArray()))
        val result = BigDecimalSerializer.read(input)
        assertEquals(BigDecimal("123.45"), result)
    }
}
