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

package org.apache.sysml.runtime.controlprogram.caching;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.sysml.parser.DataExpression;
import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.flink.data.DataSetObject;
import org.apache.sysml.runtime.instructions.spark.data.RDDObject;
import org.apache.sysml.runtime.io.FrameReader;
import org.apache.sysml.runtime.io.FrameReaderFactory;
import org.apache.sysml.runtime.io.FrameWriter;
import org.apache.sysml.runtime.io.FrameWriterFactory;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.MatrixDimensionsMetaData;
import org.apache.sysml.runtime.matrix.MatrixFormatMetaData;
import org.apache.sysml.runtime.matrix.MetaData;
import org.apache.sysml.runtime.matrix.data.FileFormatProperties;
import org.apache.sysml.runtime.matrix.data.FrameBlock;
import org.apache.sysml.runtime.matrix.data.OutputInfo;

public class FrameObject extends CacheableData<FrameBlock>
{
	private static final long serialVersionUID = 1755082174281927785L;

	private List<ValueType> _schema = null;
	
	/**
	 * 
	 */
	protected FrameObject() {
		super(DataType.FRAME, ValueType.STRING);
	}
	
	/**
	 * 
	 * @param fname
	 */
	public FrameObject(String fname) {
		this();
		setFileName(fname);
	}
	
	/**
	 * 
	 * @param fname
	 * @param meta
	 */
	public FrameObject(String fname, MetaData meta) {
		this();
		setFileName(fname);
		setMetaData(meta);
	}
	
	/**
	 * Copy constructor that copies meta data but NO data.
	 * 
	 * @param fo
	 */
	public FrameObject(FrameObject fo) {
		super(fo);
	}

	public void setSchema(String schema) {
		if( schema.equals("*") ) {
			//populate default schema
			int clen = (int) getNumColumns();
			if( clen > 0 ) //known number of cols
				_schema = Collections.nCopies(clen, ValueType.STRING);
		}
		else {
			//parse given schema
			_schema = new ArrayList<ValueType>();
			String[] parts = schema.split(DataExpression.DEFAULT_DELIM_DELIMITER);
			for( String svt : parts )
				_schema.add(ValueType.valueOf(svt.toUpperCase()));
		}
	}
	
	@Override
	public void refreshMetaData() 
		throws CacheException
	{
		if ( _data == null || _metaData ==null ) //refresh only for existing data
			throw new CacheException("Cannot refresh meta data because there is no data or meta data. "); 

		//update matrix characteristics
		MatrixCharacteristics mc = ((MatrixDimensionsMetaData) _metaData).getMatrixCharacteristics();
		mc.setDimension( _data.getNumRows(),_data.getNumColumns() );
		
		//update schema information
		_schema = _data.getSchema();
	}
	
	/**
	 * 
	 * @return
	 */
	public long getNumRows() {
		MatrixCharacteristics mc = getMatrixCharacteristics();
		return mc.getRows();
	}

	/**
	 * 
	 * @return
	 */
	public long getNumColumns() {
		MatrixCharacteristics mc = getMatrixCharacteristics();
		return mc.getCols();
	}

	@Override
	protected FrameBlock readBlobFromCache(String fname) throws IOException {
		return (FrameBlock)LazyWriteBuffer.readBlock(fname, false);
	}

	@Override
	protected FrameBlock readBlobFromHDFS(String fname, long rlen, long clen)
		throws IOException 
	{
		MatrixFormatMetaData iimd = (MatrixFormatMetaData) _metaData;
		MatrixCharacteristics mc = iimd.getMatrixCharacteristics();
		
		FrameBlock data = null;
		try {
			FrameReader reader = FrameReaderFactory.createFrameReader(
					iimd.getInputInfo());
			data = reader.readFrameFromHDFS(fname, _schema, mc.getRows(), mc.getCols()); 
		}
		catch( DMLRuntimeException ex ) {
			throw new IOException(ex);
		}
			
		//sanity check correct output
		if( data == null )
			throw new IOException("Unable to load frame from file: "+fname);
						
		return data;
	}

	@Override
	protected FrameBlock readBlobFromRDD(RDDObject rdd, MutableBoolean status)
			throws IOException 
	{
		//TODO support for distributed frame representations
		throw new IOException("Not implemented yet.");
	}

	@Override
	protected FrameBlock readBlobFromDataSet(DataSetObject dso, MutableBoolean status)
			throws IOException
	{
		//TODO support for distributed frame representations
		throw new IOException("Not implemented yet.");
	}

	@Override
	protected void writeBlobToHDFS(String fname, String ofmt, int rep, FileFormatProperties fprop) 
		throws IOException, DMLRuntimeException 
	{
		OutputInfo oinfo = OutputInfo.stringToOutputInfo(ofmt);
		FrameWriter writer = FrameWriterFactory.createFrameWriter(oinfo);
		writer.writeFrameToHDFS(_data, fname,  getNumRows(), getNumColumns());
	}

	@Override
	protected void writeBlobFromRDDtoHDFS(RDDObject rdd, String fname, String ofmt) 
		throws IOException, DMLRuntimeException 
	{
		//TODO support for distributed frame representations
		throw new IOException("Not implemented yet.");
	}

	@Override
	protected void writeBlobFromDataSetToHDFS(DataSetObject dso, String fname, String ofmt)
			throws IOException, DMLRuntimeException
	{
		//TODO support for distributed frame representations
		throw new IOException("Not implemented yet.");
	}
}
