package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.hops.AggBinaryOp;
import org.apache.sysml.lops.PartialAggregate;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.flink.functions.*;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetAggregateUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateUnaryOperator;

/**
 * Created by rparra on 22/8/17.
 */
public class AggregateUnaryFLInstruction extends UnaryFLInstruction {

    private AggBinaryOp.SparkAggType _aggtype = null;
    private AggregateOperator _aop = null;

    public AggregateUnaryFLInstruction(AggregateUnaryOperator auop, AggregateOperator aop, CPOperand in, CPOperand out, AggBinaryOp.SparkAggType aggtype, String opcode, String istr){
        super(auop, in, out, opcode, istr);
        _aggtype = aggtype;
        _aop = aop;
    }

    public static AggregateUnaryFLInstruction parseInstruction(String str)
            throws DMLRuntimeException
    {
        String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
        InstructionUtils.checkNumFields(parts, 3);
        String opcode = parts[0];

        CPOperand in1 = new CPOperand(parts[1]);
        CPOperand out = new CPOperand(parts[2]);
        AggBinaryOp.SparkAggType aggtype = AggBinaryOp.SparkAggType.valueOf(parts[3]);

        String aopcode = InstructionUtils.deriveAggregateOperatorOpcode(opcode);
        PartialAggregate.CorrectionLocationType corrLoc = InstructionUtils.deriveAggregateOperatorCorrectionLocation(opcode);
        String corrExists = (corrLoc != PartialAggregate.CorrectionLocationType.NONE) ? "true" : "false";

        AggregateUnaryOperator aggun = InstructionUtils.parseBasicAggregateUnaryOperator(opcode);
        AggregateOperator aop = InstructionUtils.parseAggregateOperator(aopcode, corrExists, corrLoc.toString());
        return new AggregateUnaryFLInstruction(aggun, aop, in1, out, aggtype, opcode, str);
    }


    @Override
    public void processInstruction(ExecutionContext ec) throws DMLRuntimeException {
        FlinkExecutionContext flec = (FlinkExecutionContext) ec;
        MatrixCharacteristics mc = flec.getMatrixCharacteristics(input1.getName());

        //get input
        DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in = flec.getBinaryBlockDataSetHandleForVariable( input1.getName() );
        DataSet<Tuple2<MatrixIndexes,MatrixBlock>> out = in;

        //filter input blocks for trace
        if( getOpcode().equalsIgnoreCase("uaktrace") )
            out = out.filter(new FilterDiagBlocksFunction());

        //execute unary aggregate operation
        AggregateUnaryOperator auop = (AggregateUnaryOperator)_optr;
        AggregateOperator aggop = _aop;

        //perform aggregation if necessary and put output into symbol table
        if( _aggtype == AggBinaryOp.SparkAggType.SINGLE_BLOCK )
        {
            DataSet<MatrixBlock> out2 = out.map(
                    new UAggFunction2(auop, mc.getRowsPerBlock(), mc.getColsPerBlock()));
            MatrixBlock out3 = DataSetAggregateUtils.aggStableSingle(out2, aggop);

            //drop correction after aggregation
            out3.dropLastRowsOrColums(aggop.correctionLocation);

            //put output block into symbol table (no lineage because single block)
            //this also includes implicit maintenance of matrix characteristics
            flec.setMatrixOutput(output.getName(), out3, getExtendedOpcode());
        }
        else //MULTI_BLOCK or NONE
        {
            if( _aggtype == AggBinaryOp.SparkAggType.NONE ) {
                //in case of no block aggregation, we always drop the correction as well as
                //use a partitioning-preserving mapvalues
                out = out.map(new UAggValueFunction(auop, mc.getRowsPerBlock(), mc.getColsPerBlock()));
            }
            else if( _aggtype == AggBinaryOp.SparkAggType.MULTI_BLOCK ) {
                //in case of multi-block aggregation, we always keep the correction
                out = out.map(new UAggFunction(auop, mc.getRowsPerBlock(), mc.getColsPerBlock()));
                out = DataSetAggregateUtils.aggByKeyStable(out, aggop);

                //drop correction after aggregation if required (aggbykey creates
                //partitioning, drop correction via partitioning-preserving mapvalues)
                if( auop.aggOp.correctionExists )
                    out = out.map( new AggregateDropCorrectionFunction(aggop) );
            }

            //put output RDD handle into symbol table
            updateUnaryAggOutputMatrixCharacteristics(flec, auop.indexFn);
            flec.setDataSetHandleForVariable(output.getName(), out);
            flec.addLineageDataSet(output.getName(), input1.getName());
        }

    }

}
