// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common.jni.vec;


import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.ColumnType.Type;

/**
 * Store a batch of data as vector table.
 */
public class VectorTable {
    private final VectorColumn[] columns;
    private final ColumnType[] columnTypes;
    private final String[] fields;
    private final VectorColumn meta;
    private final boolean onlyReadable;

    // Create writable vector table
    private VectorTable(ColumnType[] types, String[] fields, int capacity) {
        this.columnTypes = types;
        this.fields = fields;
        this.columns = new VectorColumn[types.length];
        int metaSize = 1; // number of rows
        for (int i = 0; i < types.length; i++) {
            columns[i] = VectorColumn.createWritableColumn(types[i], capacity);
            metaSize += types[i].metaSize();
        }
        this.meta = VectorColumn.createWritableColumn(new ColumnType("#meta", Type.BIGINT), metaSize);
        this.onlyReadable = false;
    }

    // Create readable vector table
    // `metaAddress` is generated by `JniConnector::generate_meta_info`
    private VectorTable(ColumnType[] types, String[] fields, long metaAddress) {
        long address = metaAddress;
        this.columnTypes = types;
        this.fields = fields;
        this.columns = new VectorColumn[types.length];

        int numRows = (int) OffHeap.getLong(null, address);
        address += 8;
        int metaSize = 1; // stores the number of rows + other columns meta data
        for (int i = 0; i < types.length; i++) {
            columns[i] = VectorColumn.createReadableColumn(types[i], numRows, address);
            metaSize += types[i].metaSize();
            address += types[i].metaSize() * 8L;
        }
        this.meta = VectorColumn.createReadableColumn(metaAddress, metaSize, new ColumnType("#meta", Type.BIGINT));
        this.onlyReadable = true;
    }

    public static VectorTable createWritableTable(ColumnType[] types, String[] fields, int capacity) {
        return new VectorTable(types, fields, capacity);
    }

    public static VectorTable createReadableTable(ColumnType[] types, String[] fields, long metaAddress) {
        return new VectorTable(types, fields, metaAddress);
    }

    public void appendNativeData(int fieldId, NativeColumnValue o) {
        assert (!onlyReadable);
        columns[fieldId].appendNativeValue(o);
    }

    public void appendData(int fieldId, ColumnValue o) {
        assert (!onlyReadable);
        columns[fieldId].appendValue(o);
    }

    public VectorColumn[] getColumns() {
        return columns;
    }

    public VectorColumn getColumn(int fieldId) {
        return columns[fieldId];
    }

    public ColumnType[] getColumnTypes() {
        return columnTypes;
    }

    public String[] getFields() {
        return fields;
    }

    public void releaseColumn(int fieldId) {
        assert (!onlyReadable);
        columns[fieldId].close();
    }

    public int getNumRows() {
        return columns[0].numRows();
    }

    public long getMetaAddress() {
        if (!onlyReadable) {
            meta.reset();
            meta.appendLong(getNumRows());
            for (VectorColumn c : columns) {
                c.updateMeta(meta);
            }
        }
        return meta.dataAddress();
    }

    public void reset() {
        assert (!onlyReadable);
        for (VectorColumn column : columns) {
            column.reset();
        }
        meta.reset();
    }

    public void close() {
        assert (!onlyReadable);
        for (int i = 0; i < columns.length; i++) {
            releaseColumn(i);
        }
        meta.close();
    }

    // for test only.
    public String dump(int rowLimit) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowLimit && i < getNumRows(); i++) {
            for (int j = 0; j < columns.length; j++) {
                if (j != 0) {
                    sb.append(", ");
                }
                columns[j].dump(sb, i);
            }
            sb.append('\n');
        }
        return sb.toString();
    }
}
