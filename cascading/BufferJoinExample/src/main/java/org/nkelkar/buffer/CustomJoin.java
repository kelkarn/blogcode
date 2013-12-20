package org.nkelkar.buffer;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import java.util.Iterator;

/**
 * User: nkelkar
 * Date: 12/20/13
 * Time: 12:19 PM
 */

public class CustomJoin extends BaseOperation implements Buffer {

    public CustomJoin(Fields fields) {
        super(fields);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {

        JoinerClosure joinerClosure = bufferCall.getJoinerClosure();

        if( joinerClosure.size() != 2 )
            throw new IllegalArgumentException( "joiner size wrong" );

        Iterator<Tuple> lhs = joinerClosure.getIterator( 0 ); // get males profile tuple iterator

        while(lhs.hasNext()) {

            Tuple lhsTuple = lhs.next(); // get a single male record

            Iterator<Tuple> rhs = joinerClosure.getIterator( 1 ); // get females profile tuple iterator

            while(rhs.hasNext()) {

                Tuple rhsTuple = rhs.next();    // get a single female record

                // implement business logic here
                if(rhsTuple.getInteger( 1 ) <= lhsTuple.getInteger( 1 )) {  // is female age <= male age ?

                    if((rhsTuple.getInteger( 2 ) + lhsTuple.getInteger( 2 ) > 110000) &&    // are their salaries within required limits?
                       (rhsTuple.getInteger( 2 ) + lhsTuple.getInteger( 2 ) < 135000)) {

                        if(lhsTuple.getInteger( 1 ) - rhsTuple.getInteger( 1 ) <= 3) {   // do they fall within the same age group?
                            bufferCall.getOutputCollector().add(new Tuple(lhsTuple.getString( 0 ), rhsTuple.getString( 0 )));
                        }

                    }

                }
            }
        }
    }
}
