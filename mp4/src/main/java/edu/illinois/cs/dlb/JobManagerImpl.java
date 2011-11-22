package edu.illinois.cs.dlb;

import java.io.File;
import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.text.NumberFormat;
import java.util.concurrent.atomic.AtomicInteger;

import edu.illinois.cs.dlb.api.ID;
import edu.illinois.cs.dlb.api.Job;

public class JobManagerImpl implements JobManager {

    private static final NumberFormat NF = NumberFormat.getInstance();
    static {
        NF.setMinimumIntegerDigits(5);
        NF.setGroupingUsed(false);
    }

    private final int id;
    private final Registry registry;
    private final String peerURI;
    private final long splitSize;
    private final File dir;
    private final AtomicInteger counter; // TODO persist

    JobManagerImpl(int id, Registry registry, String peerURI, long splitSize, File dir) {
        this.id = id;
        this.registry = registry;
        this.peerURI = peerURI;
        this.splitSize = splitSize;
        this.dir = dir;
        this.counter = new AtomicInteger(0);
    }

    @Override
    public ID submitJob(Job job) throws IOException {
        // 1. validate input - TODO
        System.out.println(job);

        // 2. create job dir
        int jobId = counter.incrementAndGet();
        ID newId = new ID(id, jobId);
        File jobDir = new File(dir, newId.toString());
        jobDir.mkdir();
        File jarFile = new File(job.getJarFile());
        FileUtil.copy(jarFile, new File(jobDir, jarFile.getName()));

        // 3. compute tasks

        return null;
    }

    private JobManager getPeer() throws RemoteException, NotBoundException {
        return (JobManager)registry.lookup(peerURI);
    }

    private static String getSplitName(File file, int num) {
        return file.getName() + "-" + NF.format(num);
    }

    public static void main(String[] args) throws AlreadyBoundException, IOException {
        Configuration config = Configuration.load();
        int id = config.getId();
        Registry registry = LocateRegistry.getRegistry(config.getRmiRegistryHost(), config.getRmiRegistryPort());
        File dir = new File(config.getLocalDir(), Integer.toString(id));
        if (!dir.isDirectory() && !dir.mkdir())
            throw new IllegalArgumentException("Failed to create job manager directory: "+dir.toString());
        JobManager manager =
            new JobManagerImpl(id, registry, "/jobmanager/" + config.getPeerId(), config.getSplitSize(), dir);
        JobManager stub =
            (JobManager)UnicastRemoteObject.exportObject(manager, config.getRmiPort());
        registry.rebind("/jobmanager/" + id, stub);
    }
}
