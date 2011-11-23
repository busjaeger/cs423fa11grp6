package edu.illinois.cs.dlb;

import static edu.illinois.cs.dlb.Path.path;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.text.NumberFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.healthmarketscience.rmiio.RemoteOutputStream;
import com.healthmarketscience.rmiio.RemoteOutputStreamClient;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.healthmarketscience.rmiio.SimpleRemoteOutputStream;

import edu.illinois.cs.dlb.Job.JobID;

public class JobManager implements JobClient, TaskManager {

    private static final NumberFormat NF = NumberFormat.getInstance();
    static {
        NF.setMinimumIntegerDigits(5);
        NF.setGroupingUsed(false);
    }

    private final ID id;
    private final Registry registry;
    private final String peerURI;
    private final long splitSize;
    private final File dir;
    private final AtomicInteger counter; // TODO persist and resume
    private final Map<JobID, Job> jobs;

    JobManager(ID id, Registry registry, String peerURI, long splitSize, File dir) {
        this.id = id;
        this.registry = registry;
        this.peerURI = peerURI;
        this.splitSize = splitSize;
        this.dir = dir;
        this.counter = new AtomicInteger(0);
        this.jobs = new ConcurrentHashMap<JobID, Job>();
    }

    @Override
    public JobID submitJob(File jobFile, File inputFile, File outputFile) throws IOException {
        // bootstrap phase

        // 1. create job
        JobID jobId = new JobID(id, counter.incrementAndGet());
        Job job = new Job(jobId);
        jobs.put(jobId, job);

        // 2. distribute job file across cluster
        Path jobPath = path(jobId);
        Path jobFilePath = jobPath.append(jobFile.getName());
        writeLocal(jobFilePath, jobFile);
        writeRemote(jobFilePath, jobFile);
        // JobDescriptor jobDescriptor = readJobDescriptor(jobFile);

        /*
         * 3. compute tasks this is to get us started. Several decisions
         * hard-coded at the moment: one input file, how to split file (assume
         * test file, split at first line feed after split size), how to
         * distribute splits across nodes (round-robin)
         */
        Path inputPath = jobPath.append("input");
        createTasks(inputPath, inputFile);

        return jobId;
    }

    /**
     * Splits the input file into a set of balanced files and creates tasks for
     * each file. The current algorithm is hard-coded to spread the splits
     * evenly across the two machines.
     * 
     * @param inputSplits
     * @param inputFile
     * @throws FileNotFoundException
     * @throws IOException
     * @throws RemoteException
     */
    private void createTasks(Path inputPath, File inputFile) throws IOException, RemoteException {
        InputStream is = new FileInputStream(inputFile);
        try {
            int num = 0;
            while (true) {
                Path split = inputPath.append("split-" + NF.format(num));
                OutputStream os = num % 2 == 0 ? openLocal(split) : openRemote(split);
                try {
                    // TODO create tasks
                    if (writeLineSplit(is, os) == -1)
                        break;
                } finally {
                    os.close();
                }
                num++;
            }
        } finally {
            is.close();
        }
    }

    /**
     * Writes a single split to the given output location. The algorithm is
     * currently hard-coded: it assumes the input is a text file and writes
     * bytes either until the maximum split size is reached or the input stream
     * is exhausted. In case the maximum split size is reached, additional bytes
     * may be written to reach the end of the current line.
     * 
     * @param is
     * @param os
     * @return
     * @throws IOException
     */
    private long writeLineSplit(InputStream is, OutputStream os) throws IOException {
        byte[] buf = new byte[4096];
        int bufSize = 4096, read;
        long remaining = splitSize;
        while (true) {
            int len = (remaining < bufSize) ? (int)remaining : bufSize;
            if ((read = is.read(buf, 0, len)) == -1)
                return -1;
            os.write(buf, 0, read);
            remaining -= read;
            // reached end of current split
            if (remaining == 0) {
                // add characters to current split until next new line
                int c = buf[read - 1];
                while (c != '\n') {
                    if ((c = is.read()) == -1)
                        return -1;
                    os.write(c);
                    remaining--;
                }
                break;
            }
        }
        return splitSize - remaining;
    }

    @Override
    public void deleteJob(JobID jobId) {
        throw new UnsupportedOperationException("TODO deleteJob");
    }

    @Override
    public RemoteOutputStream open(Path path) throws IOException {
        OutputStream os = openLocal(path);
        return new SimpleRemoteOutputStream(os).export();
    }

    @Override
    public void write(Path path, RemoteInputStream ris) throws RemoteException, IOException {
        File file = getFile(dir, path);
        InputStream is = RemoteInputStreamClient.wrap(ris);
        try {
            FileUtil.write(file, is);
        } finally {
            is.close();
        }
    }

    private void writeLocal(Path path, File file) throws IOException {
        File dest = getFile(dir, path);
        FileUtil.copy(dest, file);
    }

    private void writeRemote(Path path, File file) throws RemoteException, IOException {
        FileInputStream is = new FileInputStream(file);
        try {
            RemoteInputStream ris = new SimpleRemoteInputStream(is).export();
            getPeer().write(path, ris);
        } finally {
            is.close();
        }
    }

    private OutputStream openLocal(Path path) throws IOException {
        File file = getFile(dir, path);
        return FileUtil.open(file);
    }

    private OutputStream openRemote(Path path) throws RemoteException, IOException {
        RemoteOutputStream ros = getPeer().open(path);
        return RemoteOutputStreamClient.wrap(ros);
    }

    private TaskManager getPeer() {
        try {
            return (TaskManager)registry.lookup(peerURI);
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        } catch (NotBoundException e) {
            return null; // not registered yet
        }
    }

    private static File getFile(File dir, Path path) {
        for (String segment : path.segments())
            dir = new File(dir, segment);
        return dir;
    }

    public static void main(String[] args) throws AlreadyBoundException, IOException {
        Configuration config = Configuration.load();
        ID id = new ID(config.getId());

        // create job manager dir
        File localDir = config.getLocalDir();
        File dir = new File(localDir, id.toString());
        if (!dir.isDirectory() && !dir.mkdirs())
            throw new IllegalArgumentException("Failed to create job manager directory: " + dir.toString());

        // register service
        String registryHost = config.getRmiRegistryHost();
        int registryPort = config.getRmiRegistryPort();
        Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
        String uri = "/jobmanager/" + id;
        String peerUri = "/jobmanager/" + config.getPeerId();
        long splitSize = config.getSplitSize();
        JobClient manager = new JobManager(id, registry, peerUri, splitSize, dir);
        int rmiPort = config.getRmiPort();
        Remote stub = UnicastRemoteObject.exportObject(manager, rmiPort);

        registry.rebind(uri, stub);

        System.out.println("JobManager started on port " + rmiPort);
    }

}
