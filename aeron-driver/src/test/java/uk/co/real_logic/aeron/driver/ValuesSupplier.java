package uk.co.real_logic.aeron.driver;

import org.junit.experimental.theories.ParameterSignature;
import org.junit.experimental.theories.ParameterSupplier;
import org.junit.experimental.theories.PotentialAssignment;
import org.junit.experimental.theories.suppliers.TestedOn;

import java.util.ArrayList;
import java.util.List;

/**
 * @see org.junit.experimental.theories.suppliers.TestedOn
 * @see org.junit.experimental.theories.ParameterSupplier
 */
public class ValuesSupplier extends ParameterSupplier
{
    @Override
    public List<PotentialAssignment> getValueSources(ParameterSignature sig)
    {
        List<PotentialAssignment> list = new ArrayList<PotentialAssignment>();
        Values testedOn = sig.getAnnotation(Values.class);
        String[] values = testedOn.value();
        for (final String s : values)
        {
            list.add(PotentialAssignment.forValue("value", s));
        }
        return list;
    }
}
