package org.apache.sysml.runtime.instructions.flink.functions;

/**
 * Created by rparra on 22/8/17.
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.AggregateUnaryOperator;

/**
 * Similar to RDDUAggFunction but single output block.
 */
public class UAggFunction2 implements MapFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixBlock>
{
    private static final long serialVersionUID = 2672082409287856038L;

    private AggregateUnaryOperator _op = null;
    private int _brlen = -1;
    private int _bclen = -1;

    public UAggFunction2( AggregateUnaryOperator op, int brlen, int bclen ) {
        _op = op;
        _brlen = brlen;
        _bclen = bclen;
    }

    @Override
    public MatrixBlock map( Tuple2<MatrixIndexes, MatrixBlock> arg0 )
            throws Exception
    {
        //unary aggregate operation (always keep the correction)
        return (MatrixBlock) arg0.f1.aggregateUnaryOperations(
                _op, new MatrixBlock(), _brlen, _bclen, arg0.f0);
    }
}
