package com.xxx.xxx.zktest;

/*
 * @author windyang
 * @time 2018/12/28 17:37
 *
 * 简单实现zookeeper的主从选举
 
         <dependency>
            <groupId>org.apache.zookeeper</groupId>
            <artifactId>zookeeper</artifactId>
            <version>3.4.6</version>
        </dependency>

 **/

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

public class ElectionMaster implements Watcher, Runnable {

    private ZooKeeper zk;

    // 集群的节点ID
    private String serviceId;
    private boolean isLeader = false;
    private int counter = 0;

    public ElectionMaster(String serviceId, String hostString) throws IOException {
        this.serviceId = serviceId;
        this.zk = new ZooKeeper(hostString, 5000, this);
    }

    private void stop() throws InterruptedException {
        zk.close();
    }

    public void run() {
        while (true) {
            try {
                if (!isLeader) {
                    if (election()) {
                        System.out.println("Svr [" + serviceId + "] is elected as the new master node now");
                    } else {
                        System.out.println("Svr [" + serviceId + "] is not master, waiting for next election round");
                    }
                } else {
                    System.out.println("Svr [" + serviceId + "] is already the master node and on the " + counter + "th work round");

                    if (counter++ > 5)
                        releaseMasterNode();
                }

                Thread.sleep(2000);
            } catch (Exception e) {
                System.out.println("Exception meeting, " + e.getMessage());
                break;
            }
        }
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println("event: " + watchedEvent);
    }

    private boolean checkMasterStatus() throws InterruptedException {
        while (true) {
            try {
                Stat stat = new Stat();
                byte data[] = zk.getData("/master", false, stat);

                isLeader = new String(data).equals(serviceId);
                return true;
            } catch (KeeperException.NoNodeException e) {
                System.out.println("not master: " + e.toString());
                return false;
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    private void releaseMasterNode() throws InterruptedException {
        Stat stat;

        try {
            stat = zk.exists("/master", true);

            // 删除子节点
            if (stat != null && isLeader) {
                System.out.println("delete node /master");
                zk.delete("/master", stat.getVersion());

                this.counter = 0;
                this.isLeader = false;
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    /**
     * 选举机制实现
     */
    private boolean election() throws InterruptedException {
        while (true) {
            try {
                zk.create("/master", serviceId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (KeeperException.NodeExistsException e) {
                isLeader = false;
                break;
            } catch (KeeperException e) {
                e.printStackTrace();
            }

            if (checkMasterStatus()) {
                break;
            }
        }

        return isLeader;
    }

    public static void main(String[] args) throws Exception {
//        String svrId1 = Integer.toString(Math.abs(new Random().nextInt()));
//        String svrId2 = Integer.toString(Math.abs(new Random().nextInt()));
//        String svrId3 = Integer.toString(Math.abs(new Random().nextInt()));

        String svrId1 = "A";
        String svrId2 = "B";
        String svrId3 = "C";

        ElectionMaster e1 = new ElectionMaster(svrId1, "10.123.6.150:2181");
        ElectionMaster e2 = new ElectionMaster(svrId2, "10.123.6.150:2181");
        ElectionMaster e3 = new ElectionMaster(svrId3, "10.123.6.150:2181");

        new Thread(e1).start();
        new Thread(e2).start();
        new Thread(e3).start();

//        e1.stop();
//        e2.stop();
//        e3.stop();
    }
}

