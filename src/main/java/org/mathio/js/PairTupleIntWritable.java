
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

public class PairTupleIntWritable extends Configured implements WritableComparable {
    private Pair<Pair<Integer,Integer>,Pair<Integer,Integer>> p = null;
    public PairTupleIntWritable() {}
    public PairTupleIntWritable(Pair<Pair<Integer,Integer>,Pair<Integer,Integer>> pi) {
        p = pi;
    }
	public Pair<Pair<Integer,Integer>,Pair<Integer,Integer>> get() {
        return p;
    }
    
    public Pair<Integer,Integer> getFirst() {
    	return p.getFirst();
    }
    
    public Pair<Integer,Integer> getSecond() {
    	return p.getSecond();
    }
    public void set (Pair<Integer,Integer> p1 ,Pair<Integer,Integer> p2) {
        p = new Pair<Pair<Integer,Integer>,Pair<Integer,Integer>> (p1,p2);
    }

    public void write(DataOutput out) throws IOException {
       out.writeInt(p.getFirst().getFirst());
       out.writeInt(p.getFirst().getSecond());
       out.writeInt(p.getSecond().getFirst());
       out.writeInt(p.getSecond().getSecond());
    }

    public void readFields(DataInput in) throws IOException {
       Integer p1 = in.readInt(); Integer p2 = in.readInt();
       Integer q1 = in.readInt(); Integer q2 = in.readInt();
       
       this.set(new Pair<Integer,Integer> (p1,p2),new Pair<Integer,Integer> (q1,q2));
    }
    
    public int compareTo(Object w) {
       PairTupleIntWritable that = (PairTupleIntWritable) w;
       PairIntWritable p1 = new PairIntWritable(this.getFirst());
       PairIntWritable p2 = new PairIntWritable(this.getSecond());
       PairIntWritable q1 = new PairIntWritable(that.getFirst());
       PairIntWritable q2 = new PairIntWritable(that.getSecond());
       
       if (p1.compareTo(q1) == 0 && p2.compareTo(q2) == 0) {
		   return 0;
       }
	   else {
		   return (p1.compareTo(q1) == -1 || (p1.compareTo(q1) == 0 && p2.compareTo(q2) == -1))  ? -1 : 1;
       }
    }
	
}
