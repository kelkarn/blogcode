package org.nkelkar.buffer.bufferjoin;

import cascading.PlatformTestCase;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import org.junit.Test;
import org.nkelkar.Assert;

/**
 * User: nkelkar
 * Date: 12/20/13
 * Time: 12:35 PM
 */

/**
 * This is a small test class that runs the BufferJoinExample
 * method to run a flow definition.
 */
public class BufferJoinExampleTest {

    private static final String DELIMITER = ",";

    @Test
    public void testBufferJoinExample() throws Exception {

        //declare source/sink paths as strings
        String inputPath = "src/test/resources/input/buffer/bufferjoin/";
        String outputPath = "src/test/resources/output/buffer/bufferjoin/";
        String expectedPath = "src/test/resources/expectation/buffer/bufferjoin/";

        // create taps to connect to data
        // sources
        Tap maleProfileSource = new FileTap(new TextDelimited(true, DELIMITER), inputPath + "male_profiles.txt", SinkMode.KEEP);
        Tap femaleProfileSource = new FileTap(new TextDelimited(true, DELIMITER), inputPath + "female_profiles.txt", SinkMode.KEEP);
        // sink
        Tap recommendedMatchSink =  new FileTap(new TextDelimited(true, DELIMITER), outputPath + "matches.txt", SinkMode.REPLACE);

        // create flow def
        FlowDef bufferJoinTestFlowDef = BufferJoinExample.createFlowDefUsing(maleProfileSource, femaleProfileSource,
                                                                             recommendedMatchSink);

        new LocalFlowConnector().connect(bufferJoinTestFlowDef).complete(); // run the flow

        Assert.sameContent(outputPath + "matches.txt", expectedPath + "expectation.txt");   // test output
    }
}
