

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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.mathio.js.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class PairIntWritable extends Configured implements WritableComparable {
    private Pair<Integer,Integer> p;
    public PairIntWritable() {}
    public PairIntWritable(Pair<Integer,Integer> pi) {
        p = pi;
    }
    public Pair<Integer,Integer> get() {
        return p;
    }
    public void set (Pair<Integer,Integer> pi) {
        p = pi;
    }

    public Integer getFirst() {
    	return p.getFirst();
    }
    
    public Integer getSecond() {
    	return p.getSecond();
    }
    
    public void write(DataOutput out) throws IOException {
       out.writeInt(p.getFirst());
       out.writeInt(p.getSecond());
    }

    public void readFields(DataInput in) throws IOException {
       p.setFirst(in.readInt());
       p.setSecond(in.readInt());
    }

    public int compareTo(Object o) {
    	PairIntWritable p = (PairIntWritable) o;
        return (this.getFirst()== p.getFirst() && this.getSecond() == p.getSecond()) ? 0 : (
                (this.getFirst() < p.getFirst() || (this.getFirst() == p.getFirst() && this.getSecond() < p.getSecond())) ? -1 : 1
                );
    }
	
}
