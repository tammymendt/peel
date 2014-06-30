package eu.stratosphere.fab.sketchbook;

import com.typesafe.config.*;
import eu.stratosphere.fab.core.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.impl.Parseable;

public class JavaTest {

    static Logger logger = LoggerFactory.getLogger(JavaTest.class);

    public static void main(String... args) throws Exception {

//        ConfigParseOptions options = ConfigParseOptions.defaults().setClassLoader(JavaTest.class.getClassLoader());
//        Config c = load("reference.conf", options);
//        c = load("hdfs.conf", options).withFallback(c);
//        c = load("stratosphere.conf", options).withFallback(c);
//        c = load("mapred.conf", options).withFallback(c);
//        c = load("application.conf", options).withFallback(c);
//        c = ConfigFactory.parseString("system.default.config.slaves = [ alexander-t540 ]").withFallback(c);
//        c = ConfigFactory.systemProperties().withFallback(c);
//        c = c.resolve();
//        System.out.println(c.getStringList("system.hadoop.config.slaves").get(0));
        Application.main(args);
    }

    private static Config load(String resourceName, ConfigParseOptions options) {
        ConfigParseable p = Parseable.newResources(resourceName, options);
        ConfigObject o = p.parse(options);
        return o.toConfig();
    }
}
