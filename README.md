# muddt

MUDDT ("Mud Data Dump Tool") is a command line utility written in Kotlin that
exports table contents from an Oracle database and later reloads them.  Dumps
are written in a compact [Kryo](https://github.com/EsotericSoftware/kryo)
format which makes them easy to move between systems.

The project is distributed under the MIT License and aims to provide a simple
solution for capturing table snapshots or migrating data between instances.

## Features

- Dump tables from Oracle databases into compact `.kryo` files
- Reload data back into any accessible Oracle instance
- Configurable fetch size and batching for large datasets
- Optional automatic clearing of target tables on load

## Requirements

- Java 8 or newer
- Access to the target Oracle database
- [Gradle](https://gradle.org/) is included via the wrapper script

## Build

Use Gradle to compile the project and create the executable JAR:

```bash
./gradlew build
```

The resulting JAR will be located under `build/libs/`.

## Commands

The application exposes the `dump` and `load` commands.

### Dump

```
java -jar build/libs/muddt-1.0-SNAPSHOT.jar dump \
  -f tables.txt --dir dump --url jdbc:oracle:thin:@//host:port/service \
  -Ddb.user=user -Ddb.password=pass
```

* `-f`, `--tables-file` – file containing table names to export.
* `--dir` – existing directory where the dump files will be written.
* `--url` – JDBC connection URL.

### Load

```
java -jar build/libs/muddt-1.0-SNAPSHOT.jar load \
  --dir dump --clear commit --url jdbc:oracle:thin:@//host:port/service \
  -Ddb.user=user -Ddb.password=pass
```

* `--dir` – directory containing `.kryo` files.
* `--clear` – how to clear tables before loading (`no`, `yes` or `commit`).
* `--url` – JDBC connection URL.

### JDBC properties

JDBC properties are provided using `-D` options. At minimum set:

* `db.user` – database user name.
* `db.password` – user password.

Optionally you can define `db.schema` to explicitly specify the schema.

## Contributing

Bug reports and pull requests are welcome.  Feel free to open an issue or
submit a PR on GitHub.

## License

This project is licensed under the terms of the [MIT
License](LICENSE).
