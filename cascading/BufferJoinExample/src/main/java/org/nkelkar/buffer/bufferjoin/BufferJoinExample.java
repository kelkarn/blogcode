package org.nkelkar.buffer.bufferjoin;

import cascading.flow.FlowDef;
import cascading.operation.Debug;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.joiner.BufferJoin;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.nkelkar.buffer.CustomJoin;

/**
 * User: nkelkar
 * Date: 12/20/13
 * Time: 11:25 AM
 */

/**
 * This class implements the business logic of a simple recommender system
 * for an online matrimonial website.
 */
public class BufferJoinExample {

    // these are the only fields that both data files contain
    private static final String AGE = "age";
    private static final String NAME = "name";
    private static final String SALARY = "salary";
    private static final String MALE_NAMESPACE = "_male";
    private static final String FEMALE_NAMESPACE = "_female";

    /**
     * This method defines the sequence of data operations required
     * for this Cascading flow.
     *
     * @param maleProfileSource     source tap in src/test/resources/input/buffer/bufferjoin/male_profiles.txt
     * @param femaleProfileSource   source tap in src/test/resources/input/buffer/bufferjoin/female_profiles.txt
     * @param recommendedMatchSink  sink tap in src/test/resources/output/buffer/bufferjoin/matches.txt
     * @return                      Flow definition that a FlowConnector needs to run
     */
    public static FlowDef createFlowDefUsing(Tap<?, ?, ?> maleProfileSource, Tap<?, ?, ?> femaleProfileSource,
                                             Tap<?, ?, ?> recommendedMatchSink) {

        // create pipes to connect to sources
        Pipe males = new Pipe("male_profiles");
        Pipe females = new Pipe("female_profiles");
        Pipe matches = new Pipe("recommended_matches");

        // first, some field re-naming
        males = new Rename(males, new Fields(NAME, AGE, SALARY),
                           new Fields(NAME + MALE_NAMESPACE, AGE + MALE_NAMESPACE, SALARY + MALE_NAMESPACE));

        // just for consistency, we rename female fields too
        females = new Rename(females, new Fields(NAME, AGE, SALARY),
                new Fields(NAME + FEMALE_NAMESPACE, AGE + FEMALE_NAMESPACE, SALARY + FEMALE_NAMESPACE));

        // perform a cross join by specifying no join fields
        // handle join logic in Buffer implementation
        matches = new CoGroup(males, Fields.NONE, females, Fields.NONE, new BufferJoin()); // indicate intent to use Buffer for join strategy

        matches = new Every(matches, new CustomJoin(new Fields(NAME + MALE_NAMESPACE, NAME + FEMALE_NAMESPACE)), Fields.RESULTS); // keep result fields

        matches = new Each(matches, new Debug(true));   // print out results to console

        //connect sources, sinks and return
        return FlowDef.flowDef()
                      .addSource(males, maleProfileSource)
                      .addSource(females, femaleProfileSource)
                      .addTailSink(matches, recommendedMatchSink);
    }
}
