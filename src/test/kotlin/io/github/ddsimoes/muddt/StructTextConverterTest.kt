import io.github.ddsimoes.muddt.StructTextConverter
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.sql.Struct

class StructTextConverterTest {
    @Test
    fun `convert simple struct`() {
        val struct = mock<Struct>()
        whenever(struct.sqlTypeName).thenReturn("SIMPLE_TYPE")
        whenever(struct.attributes).thenReturn(arrayOf("a", 1))
        val result = StructTextConverter.invoke(struct)
        assertEquals("!SIMPLE_TYPE:a|1", result)
    }
}
