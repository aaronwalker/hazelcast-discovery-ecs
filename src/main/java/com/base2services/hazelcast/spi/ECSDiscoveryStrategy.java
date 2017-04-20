/*
 * Copyright (c) 2017, base2Services, Pty Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.base2services.hazelcast.spi;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.ecs.model.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.*;


public class ECSDiscoveryStrategy extends AbstractDiscoveryStrategy {

    private final String [] ecsCluster;
    private final int hazelcastPort;
    private final AmazonECS ecs;
    private final AmazonEC2 ec2;


    ECSDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties);


        this.ecsCluster = getOrDefault("ecs.clusters", ECSDiscoveryConfiguration.ECS_CLUSTER, "default").split(",");
        this.hazelcastPort = getOrDefault("ecs.hazelcastPort", ECSDiscoveryConfiguration.ECS_HAZELCAST_PORT, 5701);
        ecs = AmazonECSClientBuilder.defaultClient();
        ec2 = AmazonEC2ClientBuilder.defaultClient();
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {

        Collection<DiscoveryNode> discoveredNodes = new ArrayList<DiscoveryNode>();
        for(String cluster : ecsCluster) {
            discoveredNodes.addAll(collectNodesForCluster(cluster, hazelcastPort));
        }
        return discoveredNodes;
    }

    private Collection<DiscoveryNode> collectNodesForCluster(String cluster, int port) {
        Collection<DiscoveryNode> discoveredNodes = new ArrayList<DiscoveryNode>();

        //get all of the possible nodes a hazelcast container could be running on
        Map<String, String> ecsInstanceIpMap = collectECSInstanceIpAddresses(cluster, port);

        //Find all the running tasks for the cluster
        List<Task> runningTasks = collectRunningTasks(cluster);
        for(Task task : runningTasks) {
            String ecsHostIp = ecsInstanceIpMap.get(task.getContainerInstanceArn());
            //Find the containers that expose the hazelcast port
            List<NetworkBinding> networkBindings = collectHazelcatPorts(task, port);
            for(NetworkBinding nb : networkBindings) {
                getLogger().info("found hazelcast container " + getLocalHost() + " running at " + ecsHostIp + ":" + nb.getHostPort());
                discoveredNodes.add(mapToDiscoveryNode(ecsHostIp, nb.getHostPort()));
            }
        }
        return discoveredNodes;
    }

    private String getLocalHost()  {
        try {
            return Inet4Address.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Task> collectRunningTasks(String cluster) {
        List<Task> running = new ArrayList<Task>();
        try {
            ListTasksResult taskResult = ecs.listTasks(new ListTasksRequest().withCluster(cluster));
            if(!taskResult.getTaskArns().isEmpty()) {
                DescribeTasksResult tasks = ecs.describeTasks(new DescribeTasksRequest().withCluster(cluster).withTasks(taskResult.getTaskArns()));
                for (Task task : tasks.getTasks()) {
                    if (task.getLastStatus().equals("RUNNING")) {
                        running.add(task);
                    }
                }
            }
        } catch(ClusterNotFoundException e) {
            getLogger().warning("ECS cluster " + cluster + " was not found");
        }
        return running;
    }

    private List<NetworkBinding> collectHazelcatPorts(Task task, int port) {
        List<NetworkBinding> networkBindings =  new ArrayList<NetworkBinding>();
        for(Container container: task.getContainers()) {
            for(NetworkBinding nb : container.getNetworkBindings()) {
                if(port == nb.getContainerPort()) {
                    networkBindings.add(nb);
                }
            }
        }
        return networkBindings;
    }

    private Map<String, String> collectECSInstanceIpAddresses(String cluster, int port) {
        Map<String, String> ecsInstanceIps = new HashMap<String, String>();
        try {
            List<String> containerInstanceArns = ecs.listContainerInstances(new ListContainerInstancesRequest()
                    .withCluster(cluster)
                    .withStatus(ContainerInstanceStatus.ACTIVE)
            ).getContainerInstanceArns();
            if (!containerInstanceArns.isEmpty()) {
                List<ContainerInstance> ecsInstances = ecs.describeContainerInstances(new DescribeContainerInstancesRequest()
                        .withCluster(cluster)
                        .withContainerInstances(containerInstanceArns)
                ).getContainerInstances();
                for (ContainerInstance instance : ecsInstances) {
                    ecsInstanceIps.put(instance.getContainerInstanceArn(), lookupEC2PrivateIp(instance.getEc2InstanceId()));
                }
            }
        } catch(ClusterNotFoundException e) {
            getLogger().warning("ECS cluster " + cluster + " was not found");
        }
        return ecsInstanceIps;
    }

    private String lookupEC2PrivateIp(String ec2InstanceId) {
        DescribeInstancesResult ec2Instances = ec2.describeInstances(new DescribeInstancesRequest()
            .withInstanceIds(ec2InstanceId)
        );
        return ec2Instances.getReservations().get(0).getInstances().get(0).getPrivateIpAddress();
    }


    private DiscoveryNode mapToDiscoveryNode(String ipAddress, int port)  {
        try {
            return new SimpleDiscoveryNode(new Address(ipAddress,port));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

    }
}
