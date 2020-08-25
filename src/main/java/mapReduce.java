import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;


public class mapReduce {


    /////////////////////////////////////// MR1 + MR2////////////////////////////////////////////////////////////////////////

    public static class MapperCountRoute extends Mapper<LongWritable, Text, Trio, IntWritable> {
        // mapper
        // input:   <lineId, headWord \t w1/w1Category/w1Label/nextNode w2/w2Category/w2Label/nextNode ...... \t totalCount /t ......
        // second part example: cease/VB/ccomp/0  for/IN/prep/1  some/DT/det/4  time/NN/pobj/2
        // output: <route, word1, word2> , total count
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            if(words.length < 3){
                System.out.println("we didn't think this would happen....");
                return;
            }
            System.out.println(words[1]); //print graph
            String[] graph = words[1].split(" ");
            for (int i = 0; i < graph.length; i++){
                for (int j = 0; j < graph.length; j++){
                    String[] iNode = graph[i].split("/");
                    String[] jNode = graph[j].split("/");
                    if (i != j && isNoun(iNode[1]) && isNoun(jNode[1]) && validStr(iNode[0]) && validStr(jNode[0])){
                        String route = getRoute(graph, i, Integer.parseInt(jNode[jNode.length-1]) - 1);
                        if (route != null){
//                            System.out.println("MR1MR2-Map: word1=" + word1 + ", word2=" + word2 + ", route=" + route + ", count="+ words[2]);
                            System.out.println("word1= " + iNode[0] + ", word2= " + jNode[0] + ", route= " + route + jNode[2]);
                            context.write(new Trio(route + jNode[2], stem(iNode[0]), stem(jNode[0])), new IntWritable(Integer.parseInt(words[2]))); // words 2 = total count
                        }
                    }
                }
            }
        }

        private String stem(String s) {
            Stemmer stem = new Stemmer();
            for(int i = 0; i< s.length(); i++){
                stem.add(s.charAt(i));
            }
            stem.stem();
            return stem.toString();
        }

        private String getNodeText(String[] graph, int i){
            String[] node = graph[i].split("/");
            return node[2] + "/" + node[0] + "/";
        }

        private boolean isNoun(String s) {
            return s.equals("NN") || s.equals("NNS") || s.equals("NNP") || s.equals("NNPS");
        }

        private boolean validStr(String s) {
            Pattern pattern = Pattern.compile("^[a-zA-Z0-9]*$");
            return s != null && !s.equals("") && pattern.matcher(s).matches();
            }

        private String getRoute(String[] graph, int i, int j) {
            if(j == -1){
               return null;
           }
            if(i == j){
                return "";
            }

            String[] node = graph[j].split("/");
            int points;
            try {
                points =  Integer.parseInt(node[node.length-1]);
            }
            catch (NumberFormatException e){
                return null;
            }
            String res = getRoute(graph, i,  points-1);
            if (res == null){
                return null;
            }
            return res + getNodeText(graph,j);
        }
    }

    //reduce class
    // input:  <route, word1, word2> , [1,1,1.....]
    // output: <route, word1, word2> , local_counter || <! , ! ,!>, vector_length || <route, !, !> , route_index
    public static class ReducerCountRoute extends Reducer<Trio, IntWritable, Trio, IntWritable> {
        private int local_counter = 0;
        private String prev_route = "";
        private int unique = 0;
        private int dpMin = 0;

        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            this.dpMin = Integer.parseInt(conf.get("dpMin"));
            System.out.println("MR1MR2-Reduce! dpMin = " + dpMin);
        }
        public void reduce(Trio key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            if(prev_route.equals("")){// first time
                prev_route = key.getRoute();
            }

            if(!prev_route.equals(key.getRoute())){
                if (dpMin <= unique) {
                    System.out.println("UNIQUE: ROUTE= " + prev_route + ", uniqueVal= " + unique + ", INDEX= " + local_counter);
                    context.write(new Trio(prev_route, "!", "!"), new IntWritable(local_counter));
                    local_counter++;
                }
                else {
                    System.out.println("NOT Unique: ROUTE= " + prev_route + ", uniqueVal= " + unique);
                }
                prev_route = key.getRoute();
                unique = 0;
            }

            int sum =0;
            for (IntWritable value : values){
                sum += value.get();
            }
//            System.out.println("MR1MR2-Reduce: route=" + prev_route + ", word1=" + key.getWord1() + ", word2=" + key.getWord2() + ", sum=" + sum);
            context.write(new Trio(prev_route, key.getWord1(), key.getWord2()), new IntWritable(sum));
            unique++;
//            System.out.println("CURR_ROUTE= " + key.getRoute() + ", uniqueVal= " + unique);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            if (dpMin <= unique){
                System.out.println("MR1MR2-Reduce: UNIQUE: route=" + prev_route + ", local_counter=" + local_counter);
                context.write(new Trio(prev_route, "!", "!") , new IntWritable(local_counter));
                local_counter++;
            }
            System.out.println("MR1MR2-Reduce: FEATURE_VECTOR_LENGTH=" + local_counter);
            context.write(new Trio("!","!","!"), new IntWritable(local_counter));
        }
    }
    ///////////////////////////////////// end of MR1 + MR2////////////////////////////////////////////////////


    ////////////////////////////////////  MR3 - create the elements for the feature vector ///////////////////
    // input: <lineId, value = trio<route, word1,word2>\t<counter>>
    //output: < <route, word1,word2>, counter>
    public static class MapperElementFeature extends Mapper<LongWritable, Text, Trio, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitted = value.toString().split("\t"); // splitted[0] = key, splitted[1] = value
            String [] trioText = splitted[0].split(" ");
            if(trioText.length != 6){
              System.out.println("MR3 - trio text length is not 6, why???");
              return;
            }
            Trio trio = new Trio(trioText[1], trioText[3], trioText[5]);
            IntWritable val = new IntWritable(Integer.parseInt(splitted[1]));
            System.out.println("MR3-Map: " + trio + ", index=" + val);
            context.write(trio, val);
        }
    }

    // input:  < <!,!,!>, total_counter> || <<route, !,!>, index> || <<route, word1, word2>, sum>
    // output: <<!,!> <total_counter, -1> > || < <word1,word2> , <index, sum> >
    public static class ReducerElementFeature extends Reducer<Trio, IntWritable, PairStrings, PairInt> {
        private int local_route_index;
        private String prev_route = "";

        public void reduce(Trio key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
//            System.out.println("mr3 reduce: " + key );
            if(key.getRoute().equals("!") && key.getWord1().equals("!") && key.getWord2().equals("!")){ // total counter case
                int counter = 0;
                for( IntWritable val : values){
                    counter += val.get();
                }
                System.out.println("MR3-Reduce: FEATURE_VECTOR_LENGTH=" + counter);
                context.write(new PairStrings("!","!"), new PairInt(counter, -1));
            }
            else if(key.getWord1().equals("!") && key.getWord2().equals("!")){ // route index case
                local_route_index = 0;
                for( IntWritable val : values){
                    local_route_index = val.get();
                    break; // take the first
                }
                System.out.println("MR3-Reduce: ROUTE= " + key.getRoute() + ", INDEX= " + local_route_index);
                prev_route = key.getRoute();
            }
            else if (prev_route.equals(key.getRoute())){   // classic case, make sure we entered last if before
                int sum = 0;
                for( IntWritable val : values){
                    sum += val.get();
                }
                context.write(new PairStrings(key.getWord1(), key.getWord2()), new PairInt(local_route_index, sum));
            }
        }
    }

    ////////////////////////////////// end of MR3 /////////////////////////////////////////////////////////////

    ////////////////////////////////// MR4 - calculate the feature vector /////////////////////////////////////
    public static class MapperCalcFeature extends Mapper<LongWritable, Text, PairStrings, PairInt> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitted = value.toString().split("\t"); // splitted[0] = key, splitted[1] = value
            String[] keyText = splitted[0].split(" ");
            String[] valText = splitted[1].split(" ");
            PairStrings keyPair = new PairStrings(keyText[1], keyText[3]);
            PairInt valPair = new PairInt(Integer.parseInt(valText[1]), Integer.parseInt(valText[3]));
//            System.out.println("mr4 map write: " + keyPair + " , " + valPair );
            context.write(keyPair,valPair);
        }
    }


    public static class ReducerCalcFeature extends Reducer<PairStrings, PairInt, WordsAndBool, IntWritableArray> {
        private Map<PairStrings, Boolean> trainingSet = new HashMap<>();
        private int vector_size =0;

        public void setup(Context context){
            AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                    .withRegion("us-east-1")
                    .build();
            String S3Bucket = "lnass3"; // to be completed
            String S3Key = "hypernym.txt"; // to be completed
            S3Object object = s3.getObject(new GetObjectRequest(S3Bucket, S3Key)); // bucket, key
            S3ObjectInputStream summaryInputStream = object.getObjectContent();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(summaryInputStream));

            String line = "";
            try{
                while ((line = bufferedReader.readLine()) != null) {
                    String[] words = line.split("\t");
                    trainingSet.put(new PairStrings(words[0],words[1]), Boolean.parseBoolean(words[2]));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        public void reduce(PairStrings key, Iterable<PairInt> values, Context context) throws IOException, InterruptedException{
//            System.out.println("mr4 reduce " + key);
            if(key.getWord1().equals("!") && key.getWord2().equals("!")){ // vector size case
                for(PairInt val : values){
                    if(vector_size != 0){
                        System.out.println("mr4 reduce why not single?");
                        System.exit(-1);
                    }
                    vector_size += val.getIndex();
                }
            }
            else{
                int[] features = new int[vector_size];
                for(PairInt val : values) {
                    features[val.getIndex()] += val.getSum();
                }
                Boolean subType1 =  trainingSet.get(new PairStrings(key.getWord1(), key.getWord2()));
                Boolean subType2 =  trainingSet.get(new PairStrings(key.getWord2(), key.getWord1()));
                if(subType1 == null && subType2 == null){
                    context.write(new WordsAndBool(key.getWord1(), key.getWord2(), null), new IntWritableArray(features));
                }
                else if (subType1 != null){
                    context.write(new WordsAndBool(key.getWord1(), key.getWord2(), subType1), new IntWritableArray(features));
                }
                else {
                    context.write(new WordsAndBool(key.getWord2(), key.getWord1(), subType2), new IntWritableArray(features));
                }
            }
        }
    }

    ///////////////////////////////// end of MR4 //////////////////////////////////////////////////////////////

    public static void main (String [] args) throws Exception{
        System.out.println("started map reduce jar");
        String input_SyntaticNgrams = "s3://lnass3/biarcs.00-of-99";
        if(args.length < 2){
            return;
        }
        ////////////////// MR1 + MR2 job /////////////////////////////////////////////////////////////
            String jobNameCountRoutes = "MR1MR2";
        Configuration configCountRoutes = new Configuration();
        configCountRoutes.set("dpMin", args[1]);
        Job jobCountRoutes = Job.getInstance(configCountRoutes, jobNameCountRoutes);
        jobCountRoutes.setJarByClass(mapReduce.class);
        jobCountRoutes.setMapperClass(MapperCountRoute.class);
        jobCountRoutes.setReducerClass(ReducerCountRoute.class);
        jobCountRoutes.setOutputKeyClass(Trio.class);
        jobCountRoutes.setOutputValueClass(IntWritable.class);
//        jobCountRoutes.setInputFormatClass(SequenceFileInputFormat.class);
        jobCountRoutes.setInputFormatClass(TextInputFormat.class);
        jobCountRoutes.setOutputFormatClass(TextOutputFormat.class);
        jobCountRoutes.setNumReduceTasks(1);
        FileInputFormat.addInputPath(jobCountRoutes, new Path(input_SyntaticNgrams));
        FileOutputFormat.setOutputPath(jobCountRoutes, new Path("s3://lnass3/output/" + jobNameCountRoutes));


        ///////////////// MR3 job ////////////////////////////////////////////////////////////////////
        String jobNameCountCounters = "MR3";
        Configuration configCountCounters = new Configuration();
        Job jobCountCounters = Job.getInstance(configCountCounters, jobNameCountCounters);
        jobCountCounters.setJarByClass(mapReduce.class);
        jobCountCounters.setMapperClass(MapperElementFeature.class);
        jobCountCounters.setReducerClass(ReducerElementFeature.class);
        jobCountCounters.setMapOutputKeyClass(Trio.class);
        jobCountCounters.setMapOutputValueClass(IntWritable.class);
        jobCountCounters.setOutputKeyClass(PairStrings.class);
        jobCountCounters.setOutputValueClass(PairInt.class);
        jobCountCounters.setInputFormatClass(TextInputFormat.class);
        jobCountCounters.setOutputFormatClass(TextOutputFormat.class);
        jobCountCounters.setNumReduceTasks(1);
        FileInputFormat.addInputPath(jobCountCounters, new Path("s3://lnass3/output/" + jobNameCountRoutes));
        FileOutputFormat.setOutputPath(jobCountCounters, new Path("s3://lnass3/output/" + jobNameCountCounters));


        ////////////// MR4 job //////////////////////////////////////////////////////////////////////////
        String jobNameCalcFeature = "MR4";
        Configuration configCalcFeatures = new Configuration();
        Job jobCalcFeatures = Job.getInstance(configCalcFeatures, jobNameCalcFeature);
        jobCalcFeatures.setJarByClass(mapReduce.class);
        jobCalcFeatures.setMapperClass(MapperCalcFeature.class);
        jobCalcFeatures.setReducerClass(ReducerCalcFeature.class);
        jobCalcFeatures.setMapOutputKeyClass(PairStrings.class);
        jobCalcFeatures.setMapOutputValueClass(PairInt.class);
        jobCalcFeatures.setOutputKeyClass(WordsAndBool.class);
        jobCalcFeatures.setOutputValueClass(IntWritableArray.class);
        jobCalcFeatures.setInputFormatClass(TextInputFormat.class);
        jobCalcFeatures.setOutputFormatClass(TextOutputFormat.class);
        jobCalcFeatures.setNumReduceTasks(1);
        FileInputFormat.addInputPath(jobCalcFeatures, new Path("s3://lnass3/output/" + jobNameCountCounters));
        FileOutputFormat.setOutputPath(jobCalcFeatures, new Path("s3://lnass3/output/" + jobNameCalcFeature));


        System.out.println("job control creation");
        JobControl jobControl = new JobControl("jobControl");

        //controlled jobs

        ControlledJob ControlledJobCountRoutes = new ControlledJob(jobCountRoutes,new ArrayList<>()); // mr1mr2
        ControlledJob ControlledJobCountCounters = new ControlledJob(jobCountCounters, Arrays.asList( // mr3
                ControlledJobCountRoutes
        ));
        ControlledJob ControlledJobCalcFeatures = new ControlledJob(jobCalcFeatures,Arrays.asList( // mr4
                ControlledJobCountCounters
        ));
        jobControl.addJob(ControlledJobCountRoutes);
        jobControl.addJob(ControlledJobCountCounters);
        jobControl.addJob(ControlledJobCalcFeatures);

        Thread controller = new Thread(jobControl);
        controller.setDaemon(true);
        controller.start();

        while(!jobControl.allFinished()){
            if(!jobControl.getFailedJobList().isEmpty())
                throw new Exception("ERROR! " +jobControl.getFailedJobList().toString() );
            for(ControlledJob job : jobControl.getSuccessfulJobList()) {
                System.out.println(job.getJobName());
                jobControl.getSuccessfulJobList().clear();
            }
            System.out.println("before sleep");
            Thread.sleep(3 * 1000); // 3 secs
        }
        System.exit(jobCalcFeatures.waitForCompletion(true) ? 0 : 1);

    }


}
