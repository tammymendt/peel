package eu.stratosphere.fab.core.beans;

import eu.stratosphere.fab.core.beans.system.Lifespan;
import org.springframework.core.convert.converter.Converter;
import scala.Enumeration.Value;

public class StringToScalaEnum implements Converter<String, Value> {

    @Override
    public Value convert(String s) {
        switch (s) {
            case "SUITE":
                return Lifespan.SUITE();
            case "EXPERIMENT":
                return Lifespan.EXPERIMENT();
            case "EXPERIMENT_RUN":
                return Lifespan.EXPERIMENT_RUN();
            default:
                throw new IllegalArgumentException(s + " can not be converted to Scala Lifecycle Value!");
        }
    }

}
