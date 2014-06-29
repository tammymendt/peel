package eu.stratosphere.fab.core.beans;

import eu.stratosphere.fab.core.beans.system.Lifespan;
import org.springframework.core.convert.converter.Converter;
import scala.Enumeration.Value;

public class StringToLifespan implements Converter<String, Value> {

    @Override
    public Value convert(String s) {
        switch (s) {
            case "SUITE":
                return Lifespan.SUITE();
            case "EXPERIMENT":
                return Lifespan.EXPERIMENT();
            default:
                throw new IllegalArgumentException(s + " can not be converted to Scala Lifecycle Value!");
        }
    }

}
