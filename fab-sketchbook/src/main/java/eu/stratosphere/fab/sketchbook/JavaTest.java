package eu.stratosphere.fab.sketchbook;

import eu.stratosphere.fab.core.JavaApplication;
import eu.stratosphere.fab.core.beans.experiment.ExperimentSuite;
import eu.stratosphere.fab.extensions.beans.system.hadoop.HDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class JavaTest {

    static Logger logger = LoggerFactory.getLogger(JavaTest.class);

    public static void main(String... args) throws Exception {

        JavaApplication.main(args);

//        String experimentsConfigPath = args.length >= 1 ? args[0] : "fab-experiments.xml";
//
//        logger.info("FAB Application");
//        AbstractApplicationContext context = new ClassPathXmlApplicationContext("fab-core.xml", "fab-extensions.xml", experimentsConfigPath);
//        context.registerShutdownHook();
//
//        HDFS hdfs = context.getBean("hdfs", HDFS.class);
    }
}
