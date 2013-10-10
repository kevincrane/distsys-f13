package distsys.objects;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/9/13
 */
public class MathSequencesImpl implements MathSequences {
    // Sample implementation of a Remote class

    /**
     * Returns the nth fibonacci number (0-indexed)
     * e.g. fibonacci(4) -> 5
     *
     * @param n Which fibonacci number to return
     * @return The nth fib number
     */
    @Override
    public Long fibonacci(Integer n) {
        if (n < 0) {
            // If n is negative, return -1
            return -1l;
        }

        Long fib = 1l;
        Long first = 1l;
        Long second = 1l;

        // Starting from the first non-one fib number, compute each sequential one
        for (int i = 2; i < n; i++) {
            fib = first + second;
            first = second;
            second = fib;
        }

        return fib;
    }

    /**
     * Returns the nth prime number using the Sieve of Erasthenes
     *
     * @param n Which prime to return
     * @return The nth prime
     */
    @Override
    public Integer nthPrime(Integer n) {
        if (n < 0) {
            return -1;
        }

        boolean[] sieve = new boolean[n * 100];     // Just take a stupidly large buffer
        Arrays.fill(sieve, Boolean.TRUE);
        int count = 0;

        // Start iterating through the Sieve of Erasthenes
        int ptr = 1;
        sieve[0] = false;
        sieve[1] = false;
        while (count < n) {
            ptr++;
            if (sieve[ptr]) {
                // Found a prime!
                for (int i = ptr * 2; i < sieve.length; i += ptr) {
                    // Negate every multiple of that prime
                    sieve[i] = false;
                }
                count++;
//                System.out.println(count + " - " + ptr);
            }
        }

        System.out.println(n + "th prime is " + ptr);
        return ptr;
    }
}
