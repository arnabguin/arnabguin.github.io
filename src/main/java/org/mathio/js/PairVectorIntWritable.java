
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

import org.mathio.js.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

public class PairVectorIntWritable extends Configured implements Writable {
    private Pair<Vector<Integer>,Vector<Integer>> pv;
    public PairVectorIntWritable() {}
    public PairVectorIntWritable(Pair<Vector<Integer>,Vector<Integer>> pvv) {
        pv = pvv;
    }
    public Pair<Vector<Integer>,Vector<Integer>> get() {
        return pv;
    }
    public void set (Pair<Vector<Integer>,Vector<Integer>> pvi) {
        pv = pvi;
    }

    
    public void write(DataOutput out) throws IOException {
    
       out.writeInt(pv.getFirst().size());
       for (Integer v:pv.getFirst()) {
           out.writeInt(v);
       }
       out.writeInt(pv.getSecond().size());
       for (Integer v:pv.getSecond()) {
           out.writeInt(v);
       }
    }

    public void readFields(DataInput in) throws IOException {
       int firstSize = in.readInt();
       Vector<Integer> v = new Vector<Integer> (firstSize);
       for (int i = 0; i < firstSize; i++) {
    	   v.add(in.readInt());
       }
       pv.setFirst(v);
       
       int secondSize = in.readInt();
       v.clear();
       v = new Vector<Integer> (secondSize);
       for (int i = 0; i < secondSize; i++) {
    	   v.add(in.readInt());
       }
       pv.setSecond(v);
    }
	
}
