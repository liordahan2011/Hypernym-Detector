import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import java.io.IOException;
import java.util.Arrays;



public class main {



    public static void main (String [] args) throws IOException {




        AmazonElasticMapReduce mapReduce =
                AmazonElasticMapReduceClientBuilder.standard().withRegion("us-east-1").build();
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3://lnass3/dsp_ass3.jar")  // This should be a full map reduce application.
                .withMainClass("mapReduce")
                .withArgs(Arrays.asList(args[0]));

        StepConfig stepConfig = new StepConfig()
                .withName("dsp202_ass3")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(4)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("ass2")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withReleaseLabel("emr-5.28.0")
                .withName("dsp ass 3")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3://lnass3/logs/")
                .withVisibleToAllUsers(true)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);


    }
}
