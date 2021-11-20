import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.Collections;

public class YarnRpc {
    public static void main(String[] args) throws IOException, YarnException {
        if (args.length != 2) {
            System.out.println("Usage: java -jar YarnRpcUnauth.jar 127.0.0.1:8032 \"touch /tmp/success\"");
            return;
        }
        String cmd = args[1];
        int amMemory = 0;
        int amVCores = 0;
        Configuration conf = new YarnConfiguration();
        conf.set(YarnConfiguration.RM_ADDRESS, args[0]);
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
//        QueueInfo queueInfo = yarnClient.getQueueInfo("default");
//        System.out.println("Queue info"
//                + ", queueName=" + queueInfo.getQueueName()
//                + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
//                + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
//                + ", queueApplicationCount=" + queueInfo.getApplications().size()
//                + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

        YarnClientApplication app = yarnClient.createApplication();
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        amContainer.setCommands(Collections.singletonList(cmd));

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        appContext.setApplicationId(appId);
        appContext.setApplicationName("DMLC-YARN");
        appContext.setAMContainerSpec(amContainer);
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        if (amMemory > maxMem) {
            amMemory = maxMem;
        }
        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        if (amVCores > maxVCores) {
            amVCores = maxVCores;
        }
        Resource capability = Resource.newInstance(amMemory, amVCores);
        appContext.setResource(capability);
        appContext.setQueue("default");
        yarnClient.submitApplication(appContext);
    }

}
