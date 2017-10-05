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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.ParForProgramBlock.PartitionFormat;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.InputInfo;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OutputInfo;
import org.apache.sysml.runtime.util.MapReduceTool;
import org.apache.sysml.utils.Statistics;

/**
 * MR job class for submitting parfor remote partitioning MR jobs.
 *
 */
public class DataPartitionerRemoteFlink extends DataPartitioner
{
    private final ExecutionContext _ec;
    private final long _numRed;
    private final int _replication;

    public DataPartitionerRemoteFlink(PartitionFormat dpf, ExecutionContext ec, long numRed, int replication, boolean keepIndexes)
    {
        super(dpf._dpf, dpf._N);

        _ec = ec;
        _numRed = numRed;
        _replication = replication;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void partitionMatrix(MatrixObject in, String fnameNew, InputInfo ii, OutputInfo oi, long rlen, long clen, int brlen, int bclen)
            throws DMLRuntimeException
    {
        String jobname = "ParFor-DPSP";
        long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;

        FlinkExecutionContext ec = (FlinkExecutionContext)_ec;

        try
        {
            //cleanup existing output files
            MapReduceTool.deleteFileIfExistOnHDFS(fnameNew);
            //get input rdd
            DataSet<Tuple2<MatrixIndexes, MatrixBlock>> inDataSet = (DataSet<Tuple2<MatrixIndexes, MatrixBlock>>)
                    ec.getDataSetHandleForMatrixObject(in, InputInfo.BinaryBlockInputInfo);

            //determine degree of parallelism
            MatrixCharacteristics mc = in.getMatrixCharacteristics();

            //run spark remote data partition job
            DataPartitionerRemoteFlinkMapper dpfun = new DataPartitionerRemoteFlinkMapper(mc, ii, oi, _format, _n);
            DataPartitionerRemoteFlinkReducer wfun = new DataPartitionerRemoteFlinkReducer(fnameNew, oi, _replication);
            inDataSet.flatMap(dpfun) //partition the input blocks
                    .groupBy(0)      //group partition blocks
                    .reduceGroup(wfun);       //write partitions to hdfs
        }
        catch(Exception ex) {
            throw new DMLRuntimeException(ex);
        }

        //maintain statistics
        Statistics.incrementNoOfCompiledSPInst();
        Statistics.incrementNoOfExecutedSPInst();
        if( DMLScript.STATISTICS ){
            Statistics.maintainCPHeavyHitters(jobname, System.nanoTime()-t0);
        }
    }

}
