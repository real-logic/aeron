package uk.co.real_logic.aeron.driver;

import org.junit.experimental.theories.ParametersSuppliedBy;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.PARAMETER;

@ParametersSuppliedBy(ValuesSupplier.class)
@Retention(RetentionPolicy.RUNTIME)
@Target(PARAMETER)
public @interface Values
{
    String[] value();
}
