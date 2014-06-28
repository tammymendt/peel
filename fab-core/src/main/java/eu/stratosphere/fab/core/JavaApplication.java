package eu.stratosphere.fab.core;

import eu.stratosphere.fab.core.beans.experiment.ExperimentSuite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class JavaApplication {

    static Logger logger = LoggerFactory.getLogger(JavaApplication.class);

    @SuppressWarnings("resource")
    public static void main(String... args) throws Exception {

        String experimentsConfigPath = args.length >= 1 ? args[0] : "fab-experiments.xml";

        logger.info(String.format("Running experiments under %s", experimentsConfigPath));
        AbstractApplicationContext context = new ClassPathXmlApplicationContext("fab-core.xml", "fab-extensions.xml", experimentsConfigPath);
        context.registerShutdownHook();

        try {
            ExperimentSuite experimentSuite = context.getBean("experiments", ExperimentSuite.class);
            experimentSuite.run();
        } catch (Exception e) {
            logger.error("Error in experiment suite: " + e.getMessage());
            System.exit(-1);
        }
    }
}
