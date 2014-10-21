#include <mintomic/mintomic.h>
#include <mintpack/random.h>


static mint_atomic${TEST_INT_BITSIZE}_t object = { 0 };

bool ${TEST_FUNC}(int numThreads)
{
    Random random;
    uint${TEST_INT_BITSIZE}_t mirror = 0, operand, expected;

    for (int i = 0; i < 1000; i++)
    {
        // compare_exchange
        expected = (random.generate32() & 1) != 0 ? mirror : random.generate${TEST_INT_BITSIZE}();
        operand = random.generate${TEST_INT_BITSIZE}();
        if (mint_compare_exchange_strong_${TEST_INT_BITSIZE}_relaxed(&object, expected, operand) != mirror)
            return false;
        if (expected == mirror)
            mirror = operand;

        // fetch_add
        operand = random.generate${TEST_INT_BITSIZE}();
        if (mint_fetch_add_${TEST_INT_BITSIZE}_relaxed(&object, operand) != mirror)
            return false;
        mirror += operand;

        // fetch_and
        operand = random.generate${TEST_INT_BITSIZE}();
        if (mint_fetch_and_${TEST_INT_BITSIZE}_relaxed(&object, operand) != mirror)
            return false;
        mirror &= operand;

        // fetch_or
        operand = random.generate${TEST_INT_BITSIZE}();
        if (mint_fetch_or_${TEST_INT_BITSIZE}_relaxed(&object, operand) != mirror)
            return false;
        mirror |= operand;
    }

    return true;
}
