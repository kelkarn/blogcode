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
 * Created with IntelliJ IDEA.
 * User: nkelkar
 * Date: 12/20/13
 * Time: 12:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class BufferJoinExampleTest {

    private static final String DELIMITER = ",";

    @Test
    public void testBufferJoinExample() throws Exception {

        String inputPath = "src/test/resources/input/buffer/bufferjoin/";
        String outputPath = "src/test/resources/output/buffer/bufferjoin/";
        String expectedPath = "src/test/resources/expectation/buffer/bufferjoin/";

        Tap maleProfileSource = new FileTap(new TextDelimited(true, DELIMITER), inputPath + "male_profiles.txt", SinkMode.KEEP);

        Tap femaleProfileSource = new FileTap(new TextDelimited(true, DELIMITER), inputPath + "female_profiles.txt", SinkMode.KEEP);

        Tap recommendedMatchSink =  new FileTap(new TextDelimited(true, DELIMITER), outputPath + "matches.txt", SinkMode.REPLACE);

        FlowDef bufferJoinTestFlowDef = BufferJoinExample.createFlowDefUsing(maleProfileSource, femaleProfileSource, recommendedMatchSink);

        new LocalFlowConnector().connect(bufferJoinTestFlowDef).complete(); // run the flow

        Assert.sameContent(outputPath + "matches.txt", expectedPath + "expectation.txt");
    }
}
