/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysml.runtime.controlprogram.parfor;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.Writable;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.ParForProgramBlock.PDataPartitionFormat;
import org.apache.sysml.runtime.controlprogram.parfor.util.PairWritableBlock;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.InputInfo;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OutputInfo;
import org.apache.sysml.runtime.util.DataConverter;

import java.io.IOException;

/**
 * NOTE: for the moment we only support binary block here
 * TODO extend impl for binarycell and textcell
 *
 * Interface of Writable output in order to support both PairWritableBlock and PairWritableCell.
 *
 */
public class DataPartitionerRemoteFlinkMapper extends ParWorker implements FlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>, Tuple2<Long, Writable>>
{

    private static final long serialVersionUID = 332887624852010957L;

    private final long _rlen;
    private final long _clen;
    private final long _brlen;
    private final long _bclen;
    private PDataPartitionFormat _dpf;
    private final long _n;

    public DataPartitionerRemoteFlinkMapper(MatrixCharacteristics mc, InputInfo ii, OutputInfo oi, PDataPartitionFormat dpf, int n)
            throws DMLRuntimeException
    {
        _rlen = mc.getRows();
        _clen = mc.getCols();
        _brlen = mc.getRowsPerBlock();
        _bclen = mc.getColsPerBlock();
        _dpf = dpf;
        _n = n;
    }

    @Override
    public void flatMap(Tuple2<MatrixIndexes, MatrixBlock> arg0, Collector<Tuple2<Long, Writable>> ret)
            throws Exception
    {

        MatrixIndexes key2 =  arg0.f0;
        MatrixBlock value2 = arg0.f1;
        long row_offset = (key2.getRowIndex()-1)*_brlen;
        long col_offset = (key2.getColumnIndex()-1)*_bclen;
        long rows = value2.getNumRows();
        long cols = value2.getNumColumns();

        //bound check per block
        if( row_offset + rows < 1 || row_offset + rows > _rlen || col_offset + cols<1 || col_offset + cols > _clen )
        {
            throw new IOException("Matrix block ["+(row_offset+1)+":"+(row_offset+rows)+","+(col_offset+1)+":"+(col_offset+cols)+"] " +
                    "out of overall matrix range [1:"+_rlen+",1:"+_clen+"].");
        }

        //partition inputs according to partitioning scheme
        switch( _dpf )
        {
            case ROW_WISE: {
                MatrixBlock[] blks = DataConverter.convertToMatrixBlockPartitions(value2, false);
                for( int i=0; i<rows; i++ ) {
                    PairWritableBlock tmp = new PairWritableBlock();
                    tmp.indexes = new MatrixIndexes(1, col_offset/_bclen+1);
                    tmp.block = blks[i];
                    ret.collect(new Tuple2<Long,Writable>(new Long(row_offset+1+i),tmp));
                }
                break;
            }
            case ROW_BLOCK_WISE: {
                PairWritableBlock tmp = new PairWritableBlock();
                tmp.indexes = new MatrixIndexes(1, col_offset/_bclen+1);
                tmp.block = new MatrixBlock(value2);
                ret.collect(new Tuple2<Long,Writable>(new Long(row_offset/_brlen+1),tmp));
                break;
            }
            case ROW_BLOCK_WISE_N:{
                if( _n >= _brlen ) {
                    PairWritableBlock tmp = new PairWritableBlock();
                    tmp.indexes = new MatrixIndexes(((row_offset%_n)/_brlen)+1, col_offset/_bclen+1);
                    tmp.block = new MatrixBlock(value2);
                    ret.collect(new Tuple2<Long,Writable>(new Long(row_offset/_n+1),tmp));
                }
                else {
                    for( int i=0; i<rows; i+=_n ) {
                        PairWritableBlock tmp = new PairWritableBlock();
                        tmp.indexes = new MatrixIndexes(1, col_offset/_bclen+1);
                        tmp.block = value2.sliceOperations(i, Math.min(i+(int)_n-1, value2.getNumRows()-1),
                                0, value2.getNumColumns()-1, new MatrixBlock());
                        ret.collect(new Tuple2<Long,Writable>(new Long((row_offset+i)/_n+1),tmp));
                    }
                }
                break;
            }
            case COLUMN_WISE:{
                MatrixBlock[] blks = DataConverter.convertToMatrixBlockPartitions(value2, true);
                for( int i=0; i<cols; i++ ) {
                    PairWritableBlock tmp = new PairWritableBlock();
                    tmp.indexes = new MatrixIndexes(row_offset/_brlen+1, 1);
                    tmp.block = blks[i];
                    ret.collect(new Tuple2<Long,Writable>(new Long(col_offset+1+i),tmp));
                }
                break;
            }
            case COLUMN_BLOCK_WISE: {
                PairWritableBlock tmp = new PairWritableBlock();
                tmp.indexes = new MatrixIndexes(row_offset/_brlen+1, 1);
                tmp.block = new MatrixBlock(value2);
                ret.collect(new Tuple2<Long,Writable>(new Long(col_offset/_bclen+1),tmp));
                break;
            }
            case COLUMN_BLOCK_WISE_N: {
                if( _n >= _bclen ) {
                    PairWritableBlock tmp = new PairWritableBlock();
                    tmp.indexes = new MatrixIndexes(row_offset/_brlen+1, ((col_offset%_n)/_bclen)+1);
                    tmp.block = new MatrixBlock(value2);
                    ret.collect(new Tuple2<Long,Writable>(new Long(col_offset/_n+1),tmp));
                }
                else {
                    for( int i=0; i<cols; i+=_n ) {
                        PairWritableBlock tmp = new PairWritableBlock();
                        tmp.indexes = new MatrixIndexes(row_offset/_brlen+1, 1);
                        tmp.block = value2.sliceOperations(0, value2.getNumRows()-1,
                                i, Math.min(i+(int)_n-1, value2.getNumColumns()-1), new MatrixBlock());
                        ret.collect(new Tuple2<Long,Writable>(new Long((col_offset+i)/_n+1),tmp));
                    }
                }
                break;
            }

            default:
                throw new DMLRuntimeException("Unsupported partition format: "+_dpf);
        }
    }

}
