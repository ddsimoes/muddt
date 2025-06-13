import io.github.ddsimoes.muddt.ClearType
import io.github.ddsimoes.muddt.RawDump
import io.github.ddsimoes.muddt.RawDumpLoader
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.extension.ExtendWith
import org.testcontainers.containers.OracleContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.io.File
import java.sql.DriverManager

@Testcontainers
@TestInstance(Lifecycle.PER_CLASS)
class RawDumpIntegrationTest {
    companion object {
        @Container
        val oracle: OracleContainer = OracleContainer("gvenzl/oracle-xe:21-slim")
            .withReuse(false)
    }

    private fun props() = mapOf(
        "db.user" to oracle.username,
        "db.password" to oracle.password
    )

    @Test
    fun `dump and load simple table`() {
        val conn = DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password)
        conn.createStatement().use { st ->
            st.execute("CREATE TABLE TEST_TABLE(id NUMBER PRIMARY KEY, name VARCHAR2(50))")
            st.execute("INSERT INTO TEST_TABLE(id, name) VALUES (1, 'one')")
            st.execute("INSERT INTO TEST_TABLE(id, name) VALUES (2, 'two')")
            conn.commit()
        }

        val dumpDir = createTempDir(prefix = "dump")
        val tablesFile = File(dumpDir, "tables.txt")
        tablesFile.writeText("TEST_TABLE\n")

        val dumpOptions = RawDump.RawDumpOptions(props(), "kryo", tablesFile, dumpDir, 1000, 1000, -1, oracle.jdbcUrl)
        RawDump(dumpOptions).run()

        conn.createStatement().execute("TRUNCATE TABLE TEST_TABLE")
        conn.commit()

        val loadOptions = RawDumpLoader.Options(
            properties = props(),
            dir = dumpDir,
            batchSize = 100,
            printCount = 1000,
            limit = -1,
            commitCount = 0,
            skipNonEmpty = false,
            clear = ClearType.NO,
            offset = -1,
            memInfo = false,
            url = oracle.jdbcUrl
        )
        RawDumpLoader(loadOptions).run()

        conn.createStatement().use { st ->
            val rs = st.executeQuery("SELECT COUNT(*) FROM TEST_TABLE")
            rs.next()
            val count = rs.getInt(1)
            assertEquals(2, count)
        }
    }
}
