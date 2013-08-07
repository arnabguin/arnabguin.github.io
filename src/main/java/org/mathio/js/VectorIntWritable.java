/**
 * Copyright (c) 2013 Arnab Guin 
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mathio.js;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.conf.Configured;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

public class VectorIntWritable extends Configured implements WritableComparable {
    private Vector<Integer> v;
    public VectorIntWritable() {}
    public VectorIntWritable(Vector<Integer> vi) {
        v = vi;
    }
    public VectorIntWritable(Integer [] vi) {
        int i = 0;
        for (Integer el: vi) {
            v.add(el);
        }
    }
    public Vector get() {
        return v;
    }
    public void set (Vector<Integer> vi) {
        v = vi;
    }

    public void write(DataOutput out) throws IOException {
       out.writeInt(v.size());
       for (int j = 0; j < v.size(); j++) {
           out.writeInt(v.get(j));
       }
    }

    public void readFields(DataInput in) throws IOException {
       int s = in.readInt();
       v = new Vector<Integer> (s);
       for (int j = 0; j < s; j++) {
           v.add(in.readInt());
       }
    }
	public int compareTo(Object o) {
		VectorIntWritable other = (VectorIntWritable) o;
		return v.equals(other.get()) ? 0 : (v.size() < other.get().size() ? -1 : 1);
	}
}
