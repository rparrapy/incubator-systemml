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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.runtime.controlprogram.parfor.util.PairWritableBlock;
import org.apache.sysml.runtime.io.IOUtilFunctions;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OutputInfo;
import org.apache.sysml.runtime.matrix.mapred.MRConfigurationNames;


public class DataPartitionerRemoteFlinkReducer implements GroupReduceFunction<Tuple2<Long, Writable>, Tuple2<Long, Writable>>
{
    private static final long serialVersionUID = -7149865018683261964L;

    private final String _fnameNew;
    private final int _replication;
    private SequenceFile.Writer writer;

    public DataPartitionerRemoteFlinkReducer(String fnameNew, OutputInfo oi, int replication) {
        _fnameNew = fnameNew;
        _replication = replication;
        writer = null;
    }

    @Override
    public void reduce(Iterable<Tuple2<Long, Writable>> arg0, Collector<Tuple2<Long, Writable>> collector) throws Exception {
        //prepare grouped partition input
        Iterator<Tuple2<Long, Writable>> l = arg0.iterator();

        try
        {
            //write individual blocks unordered to output
            while( l.hasNext() ) {
                Tuple2<Long, Writable> elem = l.next();
                Long key = elem.f0;
                Writable value = elem.f1;

                //write entire partition to binary block sequence file
                SequenceFile.Writer writer = this.getWriter(key);

                PairWritableBlock pair = (PairWritableBlock) value;
                writer.append(pair.indexes, pair.block);
            }
        }
        finally {
            IOUtilFunctions.closeSilently(writer);
        }
    }

    private SequenceFile.Writer getWriter(Long key) throws IOException {
        if (this.writer == null) {
            //create sequence file writer
            Configuration job = new Configuration(ConfigurationManager.getCachedJobConf());
            Path path = new Path(_fnameNew + File.separator + key);
            FileSystem fs = IOUtilFunctions.getFileSystem(path, job);
            this.writer = new SequenceFile.Writer(fs, job, path, MatrixIndexes.class, MatrixBlock.class,
                    job.getInt(MRConfigurationNames.IO_FILE_BUFFER_SIZE, 4096),
                    (short)_replication, fs.getDefaultBlockSize(), null, new SequenceFile.Metadata());

        }

        return this.writer;
    }

}

