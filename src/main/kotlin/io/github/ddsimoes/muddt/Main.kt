package io.github.ddsimoes.muddt

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.associate
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.choice
import com.github.ajalt.clikt.parameters.types.file
import com.github.ajalt.clikt.parameters.types.int
import java.io.File

object Main {

    @JvmStatic
    fun main(args: Array<String>) {
        Muddt().subcommands(Dump(), Load()).main(args)
    }

}

class Dump : CliktCommand() {
    private val properties:  Map<String, String> by option("-D").associate()
        .help("Define a property in the format -Dkey=value (eg. -Ddb.user=user -Ddb.password=pass).")

    private val type: String by option().default("kryo")
        .help("Output format for tables (kryo or csv)")

    private val tablesFile: File by option("-f", "--tables-file").file(canBeDir = false, mustExist = true).required()
        .help("Path to a file containing the list of table names to dump, one per line.")

    private val dir: File by option().file(mustExist = true, canBeFile = false).default(File("."))
        .help("Path to output dir (must exists).")

    private val fetchSize: Int by option("--fetch-size").int().default(10_000)
        .help("ResultSet fetch size (default 10000)")

    private val printCount: Int by option("--print-count").int().default(10_000)
        .help("How many read row to print progress (default 10000)")

    private val limit: Int by option().int().default(-1)
        .help("Max number of rows to read per table.")

    private val url: String by option().required().help("Database (JDBC) URL.")

    override fun run() {
        val options = RawDump.RawDumpOptions(properties, type, tablesFile, dir, fetchSize, printCount, limit, url)
        RawDump(options).run()
    }
}

class Load : CliktCommand() {
    private val properties:  Map<String, String> by option("-D").associate()
        .help("Define a property in the format -Dkey=value (eg. -Ddb.user=user -Ddb.password=pass).")

    private val dir: File by option().file(mustExist = true, canBeFile = false).default(File("."))
        .help("Path containing table files (*.kryo)")

    private val batchSize: Int by option("--batch-size").int().default(1_000)
        .help("Batch size (default 10000)")

    private val printCount: Int by option("--print-count").int().default(10_000)
        .help("How many read row to print progress (default 10000)")

    private val limit: Int by option().int().default(-1)
        .help("Max number of rows to read per table.")

    private val url: String by option().required().help("Database (JDBC) URL.")

    private val commitCount: Int by option("--commit").int().default(0)
        .help("Commit every N inserts. (0/default: commit every table; -1: do not commit/dry run)")

    private val skipNonEmpty: Boolean by option().flag(default = true)

    private val clear: String by option().choice("no", "yes", "commit").required()
        .help("Clear tables (delete from TABLE) before loading (no, yes, commit).")

    private val offset: Int by option().int().default(-1)

    private val memInfo: Boolean by option().flag(default = false)

    override fun run() {
        val options = RawDumpLoader.Options(properties, dir, batchSize, printCount, limit, commitCount, skipNonEmpty, ClearType.valueOf(clear.uppercase()), offset, memInfo, url)
        RawDumpLoader(options).run()
    }
}

class Muddt : CliktCommand() {

    override fun run() {
    }
}