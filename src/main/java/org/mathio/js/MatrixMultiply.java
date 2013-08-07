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

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;
import java.util.Vector;
import java.util.regex.Pattern;
import java.lang.String;
import java.lang.Integer;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.lang.StringUtils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.mathio.js.Pair;
import org.mathio.js.PairTupleIntWritable;
import org.mathio.js.VectorIntWritable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.MathException;

/**
 * Class representing a matrix or multiple matrices
 * <p>
 * Formats supported:
 * - One matrix per file (rows in each line, columns delimited by space
 * - Multiple matrices in one file (matrices separated by empty line)
 * @param
 * @return
 * @see  
 */

class Matrix extends MultiFileInputFormat<LongWritable,Text> {
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
        try {
			return new MatrixRecordReader(split, job);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
    }
}

/**
 * Custom record reader for matrix reads
 * Input directory provided by user may contain:
 * - One file containing multiple matrices
 * - Multiple files each containing one matrix
 * - Multiple files each containing multiple matrices
 * Eg. <user input dir>
 *     |
 *     |
 *     -- m_a
 *     -- m_b
 *     -- m_c
 * pi(f) = product of matrices in file f
 * Output matrix = pi(m_a) * pi(m_b) * pi(m_c)
 * Files are read in lexicographical order (in this case sorted order is m_a,then m_b and m_c)
 * <p>
 * @param
 * @return
 * @see  
 */

class MatrixRecordReader implements RecordReader<LongWritable, Text> {
	  
    private MultiFileSplit fileSplit;
    private Configuration conf;
    private boolean processed = false;
    private boolean processedAll = false;
    private static int nSplits = -1;
  
    private LongWritable key = new LongWritable ();
    private Text value = new Text ();
 
    private static ArrayDeque<String> values = new ArrayDeque<String> ();
    private static long keyCounter = 0L;
    private static boolean nextIter = false;
    
    private IOFileFilter nf = FileFilterUtils.and(FileFileFilter.FILE,FileFilterUtils.nameFileFilter("part-00000"));

    public MatrixRecordReader(InputSplit inputSplit, JobConf conf) throws IOException, InterruptedException {
        this.fileSplit = (MultiFileSplit) inputSplit;
        this.conf = conf;
        if (nSplits == -1) {
            Path inputdir = Matrix.getInputPaths(conf)[0];
            if (inputdir.toString().endsWith("0")) {
            	nSplits = Matrix.getInputPaths(conf)[0].getFileSystem(conf).listStatus(Matrix.getInputPaths(conf)[0]).length; 
                nextIter = false;
            } 
            else {
                //nSplits = Matrix.getInputPaths(conf)[0].getFileSystem(conf).globStatus(new Path ("part-00000")).length; // only part-00000 used for now
                nSplits = 1;
                nextIter = true;
            }
        }
    }

/**
 * Handles splits within files if multiple matrices are present
 * <p>
 * @param file offset
 * @param content of file
 * @return more records present
 * @see  
 */
    public boolean next(LongWritable key, Text value) throws IOException {
    	if (keyCounter == nSplits) {
    		nSplits = -1;
    		values.clear();
    		keyCounter = 0L;
        	return false;
        }
        if (!processed) {
            Path file = null;
            if (nextIter) {
                int numPaths = fileSplit.getNumPaths();
                int n = 0;
                while (n < numPaths) {
                    file = fileSplit.getPath(n);
                    if (Pattern.matches("^part",file.toString())) {
                        break;
                    }
                    n++;
                }
                if (file == null) {
                    throw new IOException("File system internal error");
                }
            }
            else {
                file = fileSplit.getPath((int) keyCounter);
            }

            FileSystem fs = file.getFileSystem(conf);
 
            FSDataInputStream in = null;
            
            in = fs.open(file);
            for (String paragraph: IOUtils.toString(in).split("\\n\\n")) {
                values.addFirst(paragraph);
            }
            in.close();
        } 
        key.set(keyCounter++);
        value.set(values.removeLast());
        
        if (values.isEmpty()) {
        	processed = false;
        } else {
        	nSplits = nSplits + 1;
                processed = true;
        }
        return true;
    }
 
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }
 
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }
    
    public float getProgress() throws IOException  {
        return (keyCounter == nSplits) ? 1.0f : 0.0f;
    }
 
    public void close() throws IOException {
        // do nothing
    }

	public LongWritable createKey() {
		return new LongWritable();
	}

	public Text createValue() {
		return new Text ();
	}

	public long getPos() throws IOException {
		return 0;
	}
}

/**
 * Main class for multiplication
 * <p>
 * For odd number of matrices, multiply last by identity matrix
 * eg. 3 matrices A,B,C
 * product = (A*B) * (C*I)
 */

public class MatrixMultiply {

  public static final Log LOG = LogFactory.getLog(MatrixMultiply.class);

  public static class MatrixCompute {
      private static int globalIterations = -1;
      public static void setIterations(int iter) {
          LOG.info(String.format("Global iterations = %d",iter));
    	  globalIterations = iter;
      }
      public static void resetIterations() {
          globalIterations = -1;
      }
      public static int getIterations() {
    	  return globalIterations;
      }
      
      private static int [][] generateIdentityMatrix(int n) {
          int [][] identityMatrix = new int [n][n];
          for (int i = 0; i < n; i++) {
              for (int j = 0; j < n; j++) {
                  if (j == i) {
                      identityMatrix[i][j] = 1;
                  }
                  else {
                      identityMatrix[i][j] = 0;
                  }
              }
          }
          return identityMatrix;
      }

      public static ArrayList<Pair<Integer, Integer>> computePairWiseGroups(int n) {
    	  ArrayList<Pair<Integer,Integer>> v = new ArrayList<Pair<Integer,Integer>> (n);
          if (n == 0) {
                return null;
          } else if (n == 1) {
              v.add(new Pair<Integer,Integer>(0,1));
          } else {
              for (int j = 0; j < n; j++) {
                  v.add(new Pair<Integer,Integer>(2*j,2*j+1));
              }
          }
          return v;
      }

      public static int sumVectorPair(int [] a, int [] b) {
      	int sum = 0;
          for (int i = 0; i < a.length; i++) {
              sum = sum + a[i]*b[i];
          }
          return sum;
      }
      
      public static int computeNumberOfGlobalIterations(int n) {
          return (int) Math.ceil((double)Math.log(n)/(double)Math.log(2));
      }
      public static int computeNumberOfLocalIterations(int n) {
          return (int) Math.ceil((double)n/2.0);
      }
  }
 
  // Mapper 
  public static class RowColumnMultiply extends MapReduceBase implements Mapper<LongWritable, Text, PairTupleIntWritable, VectorIntWritable> {
   
    private JobConf conf;
    public RowColumnMultiply() {} 
    public void configure(JobConf job) {
        conf = job;
    }
 
    private static ArrayList<String> matrices = new ArrayList<String> (1);
    private static OutputCollector<PairTupleIntWritable,VectorIntWritable> out = null;
    
    public void map (LongWritable key, Text value, OutputCollector<PairTupleIntWritable,VectorIntWritable> output, Reporter reporter ) throws IOException {
 
      out = output;
      LOG.info(String.format("Matrix number = %d\n", key.get()));
      LOG.info(String.format("Matrix length in rows = %d\n", value.toString().split("\\n").length));
      
      LOG.info("Mapper started");
      matrices.add(value.toString());
      
      /*

o = matrix (m(i) x n(i)), i=node number,m=row vector,n=column vector

       o  ^ 
     /    |
   o      | Local iterations (breadth of subtree)
  /  \    |
o   o o   v
 \ /      ^
  o       | Local iterations (breadth of subtree)
   \      |
    o     v
<--------->
Global iterations(depth of entire tree)

Tree is full and complete iff number of leaves L is even
If L is odd, we plug in an identity matrix at the rightmost node of the multiplication tree to make L even. 

      */
    }
    @Override
    public void close() throws IOException {
      if (matrices.size() == 0) {
    	  try {
    	      throw new MathException("Error:MatrixMultiply:No matrices provided. Nothing to do.");
    	  }
    	  catch(Exception e) {
    		  e.printStackTrace();
    	  }
      }
      int localIterations = MatrixCompute.computeNumberOfLocalIterations(matrices.size());
      LOG.info(String.format("Local iterations = %d, Num matrices to multiply = %d\n",localIterations,matrices.size()));
      if (localIterations == 0) {
    	  LOG.info("Reached convergence");
    	  return;
      }
      
      if (MatrixCompute.getIterations() == -1) {
          MatrixCompute.setIterations(MatrixCompute.computeNumberOfGlobalIterations(matrices.size()));
      }
      
      ArrayList<Pair<Integer,Integer>> pairwiseGroups = MatrixCompute.computePairWiseGroups(localIterations);
      int [][][] elements = new int [(matrices.size() % 2 == 0) ? matrices.size() : matrices.size()+1][1][1];
      int m = 0;
      
      int prevnumcolumns = 0;
      for (String matrix: matrices) {
          String [] rows = matrix.split(Pattern.quote("\n"));
          int numrows = rows.length;
          int numcolumns = rows[0].split("\\s+").length;
          if (m != 0 && numrows != prevnumcolumns) {
        	  try {
				  throw new MathException(String.format("Error:MatrixMultiply:Matrix %d not compatible for multiplication with matrix %d", m, m - 1));
			  } 
        	  catch (Exception e) {
        		  e.printStackTrace();
        	  }
          }
          prevnumcolumns = numcolumns;
          if (m % 2 == 0) {
        	  elements[m] = new int [numrows][numcolumns];
        	  int rowno = 0;
        	  int columnno = 0;
              for (String row : rows) {
                  String [] columns = row.split("\\s+");
                  for (String el: columns) {
                      elements[m][rowno][columnno++] = Integer.parseInt(el);
                  }
                  rowno++;
                  columnno = 0;
              }
          } else { 
        	  elements[m] = new int [numcolumns][numrows];
              int rowno = 0;
              int columnno = 0;
              for (String row : rows) {
            	  columnno = 0;
                  for (String el: row.split("\\s+")) {
                      elements[m][columnno++][rowno] = Integer.parseInt(el);
                  }
                  rowno++;
              }
          }
          m++;
      }

      if (m % 2 == 1) {  // odd number of matrices
          int [][] identityMatrix = MatrixCompute.generateIdentityMatrix(elements[m-1][0].length);
          elements[m] = identityMatrix;
      }

      
      for (Pair<Integer,Integer> p: pairwiseGroups) {
    	  Vector<Vector<Integer>> elrows = new Vector<Vector<Integer>> (elements[p.getFirst()].length);
          for (int i = 0; i < elements[p.getFirst()].length ; i++) {
        	  elrows.add(new Vector<Integer> (elements[p.getSecond()].length));
        	  Vector<Integer> elrow = elrows.elementAt(i);
        	  for (int j = 0; j < elements[p.getSecond()].length; j++) {
        		  elrow.add(MatrixCompute.sumVectorPair(elements[p.getFirst()][i],elements[p.getSecond()][j])); 
              }
              PairTupleIntWritable pt = new PairTupleIntWritable (new Pair<Pair<Integer,Integer>,Pair<Integer,Integer>>(p, new Pair<Integer,Integer>(Integer.valueOf(i),elements[p.getSecond()].length)));
              out.collect(pt, new VectorIntWritable(elrows.get(i)));
          }
      }
      out = null;
      matrices.clear();
   
   }             
  }

  // Reducer  

  public static class RowColumnSum 
       extends MapReduceBase implements Reducer<PairTupleIntWritable,VectorIntWritable,NullWritable,Text> {
    static private StringBuffer s  = new StringBuffer();

    static private int currentRow = -1;
    static private int currentMultiplicandMatrix = 0;

    RowColumnSum() {}
    static private NullWritable nullKey = null;

    static private OutputCollector<NullWritable,Text> out = null;
    
    public void reduce (PairTupleIntWritable key, Iterator<VectorIntWritable> values, OutputCollector<NullWritable,Text> output, Reporter reporter ) throws IOException {
      int sum = 0;
      if (out == null) {
    	  out = output;
      }
      LOG.info(String.format("Matrix %d x Matrix %d: Row=%d,NumColumns=%d\n", key.getFirst().getFirst(), key.getFirst().getSecond(), key.getSecond().getFirst(),key.getSecond().getSecond()));
      int nextRow = key.getSecond().getFirst();
      int nextMultiplicandMatrix = key.getFirst().getFirst();
      
      if (nextMultiplicandMatrix != currentMultiplicandMatrix) {
    	 out.collect( nullKey, new Text (s.toString())); 
    	 s.delete(0, s.length());
         currentMultiplicandMatrix = nextMultiplicandMatrix;
         currentRow = -2;
      }
      
      if (nextRow != currentRow) {
    	 Vector row = values.next().get();
    	 if (currentRow != -1) { 
    		 s.append("\n");
    	 }
    	 s.append(StringUtils.join(row.toArray(), " "));
    	 currentRow = nextRow;
      }
    }
    
    @Override
    public void close() throws IOException {
        out.collect( nullKey, new Text (s.toString()));
        s.delete(0, s.length());
        currentRow = -1;
        currentMultiplicandMatrix = 0;
        out = null;
    }
  }

  public static JobConf createJob(int jobid, Configuration conf, String inputDir, String outputDir) throws URISyntaxException, IOException {
	  JobConf job = new JobConf(conf, MatrixMultiply.class);
	  job.setJobName("MatrixMultiply" + "-" + jobid);

	  job.setJarByClass(MatrixMultiply.class);
	  job.setMapperClass(MatrixMultiply.RowColumnMultiply.class);
	  job.setReducerClass(MatrixMultiply.RowColumnSum.class);

	  job.setMapOutputKeyClass(PairTupleIntWritable.class);
	  job.setMapOutputValueClass(VectorIntWritable.class);
	    
	  job.setOutputKeyClass(NullWritable.class);
	  job.setOutputValueClass(Text.class);

	  job.setInputFormat(Matrix.class);
	  job.setOutputFormat(TextOutputFormat.class);
	    
	  FileInputFormat.setInputPaths(job, new Path(new URI(inputDir)));
	  FileOutputFormat.setOutputPath(job, new Path(new URI(outputDir)));
 
	  return job;
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: matrixmultiply <input file> <output file>");
      System.exit(2);
    }
    
    URI outIterDir = null;
    URI inIterDir = null;
    URI recentoutIterDir = null;
    int iteration = 0;

    URI inputPath = new URI(new Path (otherArgs[0]).toString());
    String inputPathPrefix, inputPathSuffix;
    if (otherArgs[0].contains("/")) {
    	if (otherArgs[0].endsWith("/")) {
        	otherArgs[0] = otherArgs[0].substring(0,otherArgs[0].length() - 1);
        }
        inputPathPrefix = otherArgs[0].substring(0,inputPath.toString().lastIndexOf("/") + 1);
        inputPathSuffix = otherArgs[0].substring(inputPath.toString().lastIndexOf("/") + 1);
    }
    else {
    	inputPathPrefix = new String ("");
    	inputPathSuffix = otherArgs[0];
    }
    
    String outputPathPrefix,outputPathSuffix;
    URI outputPath = new URI(new Path (otherArgs[1]).toString());
    if (otherArgs[1].contains("/")) {
    	if (otherArgs[1].endsWith("/")) {
        	otherArgs[1] = otherArgs[1].substring(0,otherArgs[1].length() - 1);
        }
        outputPathPrefix = otherArgs[1].substring(0,outputPath.toString().lastIndexOf("/") + 1);
        outputPathSuffix = otherArgs[1].substring(outputPath.toString().lastIndexOf("/") + 1);
    }
    else {
    	outputPathPrefix = new String ("");
    	outputPathSuffix = otherArgs[1];
    }
    String inDir = inputPathPrefix + inputPathSuffix + "-input-iteration-";
    String outDir = outputPathPrefix + outputPathSuffix + "-output-iteration-";
    
    JobConf job = createJob(0,conf,inDir + '0',outDir + '0');
    FileSystem fs = FileInputFormat.getInputPaths(job)[0].getFileSystem(conf);
    fs.delete(new Path(new URI(inDir + '0')),true);
    FileUtil.copy(fs,new Path(inputPath), fs,new Path (new URI(inDir + '0')),false,conf); 
    fs.delete(new Path(new URI(outDir + '0')),true);
    fs.delete(new Path(outputPath), true);
    
    JobClient.runJob(job);
    
    JobConf [] jobconfs = new JobConf [MatrixCompute.getIterations() + 1];
    recentoutIterDir = new URI(outDir + '0');
    
    for (iteration = 1; iteration < MatrixCompute.getIterations(); iteration++) {
    	fs.delete(new Path(new URI(inDir + iteration)),true);
    	fs.delete(new Path(new URI(outDir + iteration)),true);
    	inIterDir = new URI(inDir + iteration);
    	FileUtil.copy(fs,new Path(recentoutIterDir), fs,new Path (inIterDir),false,conf); 
    	outIterDir = new URI(outDir + iteration); 
        recentoutIterDir = outIterDir;
        jobconfs[iteration] = createJob(iteration, conf, inIterDir.toString(), outIterDir.toString());
        JobClient.runJob(jobconfs[iteration]); 
    }
    FileUtil.copy(fs,new Path(recentoutIterDir), fs,new Path (outputPath),false,conf); 
    
    LOG.info(String.format("Output matrix present in %s\n", outputPath.toString()));
  }
}
