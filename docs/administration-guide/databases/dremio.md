## Working with Dremio in Metabase _(Experimental)_

### Downloading the Dremio JDBC Driver JAR

You can download the JDBC driver from [Dremio's downloads page](https://www.dremio.com/download/).

### Adding the Dremio JDBC Driver JAR to the Metabase Plugins Directory

Metabase will automatically make the Dremio driver available if it finds the Dremio JDBC driver JAR in the Metabase plugins directory when it starts up.
All you need to do is create the directory, move the JAR you just downloaded into it, and restart Metabase.

By default, the plugins directory is called `plugins`, and lives in the same directory as the Metabase JAR.

For example, if you're running Metabase from a directory called `/app/`, you should move the Dremio JDBC driver JAR to `/app/plugins/`:

```bash
# example directory structure for running Metabase with Dremio support
/app/metabase.jar
/app/plugins/dremio-jdbc-driver-2.0.5-201806021755080191-767cfb5.jar
```

If you're running Metabase from the Mac App, the plugins directory defaults to `~/Library/Application Support/Metabase/Plugins/`:

```bash
# example directory structure for running Metabase Mac App with Dremio support
/Users/camsaul/Library/Application Support/Metabase/Plugins/dremio-jdbc-driver-2.0.5-201806021755080191-767cfb5.jar
```

Finally, you can choose a custom plugins directory if the default doesn't suit your needs by setting the environment variable `MB_PLUGINS_DIR`.
