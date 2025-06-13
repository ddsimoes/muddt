import io.github.ddsimoes.muddt.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.oracle.OracleContainer
import java.nio.file.Files
import java.sql.DriverManager

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RawDumpLoaderTest {
    @Container
    private val oracle = OracleContainer("gvenzl/oracle-free:23.4-slim-faststart")

    private val props: Map<String, String> by lazy {
        mapOf(
            "db.user" to oracle.username,
            "db.password" to oracle.password
        )
    }

    @BeforeEach
    fun setUp() {
        val conn = DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password)
        conn.autoCommit = false
        conn.createStatement().use { stmt ->
            try {
                stmt.executeUpdate("DROP TABLE test_table")
            } catch (_: Exception) {
            }
            stmt.executeUpdate("CREATE TABLE test_table (id NUMBER PRIMARY KEY, name VARCHAR2(50))")
            stmt.executeUpdate("INSERT INTO test_table (id, name) VALUES (1, 'foo')")
            conn.commit()
        }
        conn.close()
    }

    @Test
    fun `dump and load table`() {
        val tablesFile = Files.createTempFile("tables", ".txt").toFile().apply { writeText("test_table\n") }
        val dumpDir = Files.createTempDirectory("dump").toFile()

        val dumpOptions = RawDump.RawDumpOptions(
            props,
            "kryo",
            tablesFile,
            dumpDir,
            fetchSize = 1000,
            printCount = 1000,
            limit = -1,
            url = oracle.jdbcUrl
        )
        RawDump(dumpOptions).run()

        DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password).use { conn ->
            conn.createStatement().executeUpdate("TRUNCATE TABLE test_table")
            //conn.commit()
        }

        val loadOptions = RawDumpLoader.Options(
            props,
            dumpDir,
            batchSize = 1000,
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

        DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password).use { conn ->
            val count = conn.createStatement().executeQuery("SELECT count(*) FROM test_table").use { rs ->
                rs.next()
                rs.getInt(1)
            }
            Assertions.assertEquals(1, count)
        }
    }

}
