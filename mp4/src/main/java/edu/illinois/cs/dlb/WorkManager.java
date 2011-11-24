package edu.illinois.cs.dlb;

import static edu.illinois.cs.dlb.util.Path.path;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.healthmarketscience.rmiio.RemoteOutputStream;
import com.healthmarketscience.rmiio.RemoteOutputStreamClient;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.healthmarketscience.rmiio.SimpleRemoteOutputStream;

import edu.illinois.cs.dlb.Job.JobID;
import edu.illinois.cs.dlb.Task.TaskID;
import edu.illinois.cs.dlb.TaskStatus.Status;
import edu.illinois.cs.dlb.util.FileUtil;
import edu.illinois.cs.dlb.util.ID;
import edu.illinois.cs.dlb.util.Path;

/**
 * TODO recover from common failures
 */
public class WorkManager implements JobManager, TaskManager {

    private static final NumberFormat NF = NumberFormat.getInstance();
    static {
        NF.setMinimumIntegerDigits(5);
        NF.setGroupingUsed(false);
    }

    private final ID id;
    private final Registry registry;
    private final String remoteURI;
    private final long splitSize;
    private final File dir;
    private final AtomicInteger counter; // TODO persist and resume
    private final Map<JobID, Job> jobs;
    private final Collection<Task> remoteTasks;
    private final BlockingQueue<Task> taskQueue;

    WorkManager(ID id, Registry registry, String remoteURI, long splitSize, File dir) {
        this.id = id;
        this.registry = registry;
        this.remoteURI = remoteURI;
        this.splitSize = splitSize;
        this.dir = dir;
        this.counter = new AtomicInteger(0);
        this.jobs = new ConcurrentHashMap<JobID, Job>();
        this.taskQueue = new LinkedBlockingQueue<Task>();
        this.remoteTasks = Collections.synchronizedList(new ArrayList<Task>());
    }

    BlockingQueue<Task> getTaskQueue() {
        return taskQueue;
    }

    public File getFile(Path path) {
        File file = dir;
        for (String segment : path.segments())
            file = new File(file, segment);
        return file;
    }

    @Override
    public JobID submitJob(File jarFile, File inputFile) throws IOException {
        // bootstrap phase

        // 1. create job
        JobID jobId = new JobID(id, counter.incrementAndGet());
        Path jobPath = path(jobId);
        Path jar = jobPath.append(jarFile.getName());
        JarDescriptor descriptor = JarDescriptor.read(jarFile);
        Job job = new Job(jobId, jar, descriptor);
        jobs.put(jobId, job);

        // 2. distribute job file across cluster
        writeLocal(jar, jarFile);
        writeRemote(jar, jarFile);

        /*
         * 3. compute tasks this is to get us started. Several decisions
         * hard-coded at the moment: one input file, how to split file (assume
         * test file, split at first line feed after split size), how to
         * distribute splits across nodes (round-robin)
         */
        Path inputDir = jobPath.append("input");
        Path outputDir = jobPath.append("output");
        submitTasks(job, inputDir, outputDir, inputFile);

        return jobId;
    }

    /**
     * Splits the input file into a set of balanced files and creates tasks for
     * each file. The current algorithm is hard-coded to spread the splits
     * evenly across the two machines.
     * 
     * @param job
     * @param inputFile
     * @throws FileNotFoundException
     * @throws IOException
     * @throws RemoteException
     */
    private void submitTasks(Job job, Path inputDir, Path outputDir, File inputFile) throws IOException,
        RemoteException {
        InputStream is = new FileInputStream(inputFile);
        try {
            int num = 0;
            boolean eof;
            while (true) {
                String split = "split-" + NF.format(num);
                Path input = inputDir.append(split);
                Path output = outputDir.append(split);
                boolean remote = num % 2 == 1; // hard coded policy
                OutputStream os = remote ? openRemote(input) : openLocal(input);
                try {
                    eof = writeLineSplit(is, os) == -1;
                    TaskID id = new TaskID(job.getId(), num);
                    Task task = new Task(id, remote, input, output, job.getJar(), job.getDescriptor());
                    job.getTasks().add(task);
                    if (remote)
                        submitTaskRemote(task);
                    else
                        submitTaskLocal(task);
                } finally {
                    os.close();
                }
                if (eof)
                    break;
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
    public void submitTask(Task task) throws RemoteException {
        remoteTasks.add(task);
        submitTaskLocal(task);
    }

    public void submitTaskRemote(Task task) throws RemoteException {
        task.getStatus().setStatus(Status.TRANSFERRING);
        getRemote().submitTask(task);
    }

    public void submitTaskLocal(Task task) {
        task.getStatus().setStatus(Status.WAITING);
        taskQueue.add(task);
    }

    @Override
    public RemoteOutputStream open(Path path) throws IOException {
        OutputStream os = openLocal(path);
        return new SimpleRemoteOutputStream(os).export();
    }

    private OutputStream openLocal(Path path) throws IOException {
        File file = getFile(path);
        return FileUtil.open(file);
    }

    private OutputStream openRemote(Path path) throws RemoteException, IOException {
        RemoteOutputStream ros = getRemote().open(path);
        return RemoteOutputStreamClient.wrap(ros);
    }

    @Override
    public void write(Path path, RemoteInputStream ris) throws RemoteException, IOException {
        File file = getFile(path);
        InputStream is = RemoteInputStreamClient.wrap(ris);
        try {
            FileUtil.write(file, is);
        } finally {
            is.close();
        }
    }

    private void writeLocal(Path path, File file) throws IOException {
        File dest = getFile(path);
        FileUtil.copy(dest, file);
    }

    private void writeRemote(Path path, File file) throws RemoteException, IOException {
        FileInputStream is = new FileInputStream(file);
        try {
            RemoteInputStream ris = new SimpleRemoteInputStream(is).export();
            getRemote().write(path, ris);
        } finally {
            is.close();
        }
    }

    private TaskManager getRemote() {
        try {
            return (TaskManager)registry.lookup(remoteURI);
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        } catch (NotBoundException e) {
            return null; // not registered yet
        }
    }

    public static void main(String[] args) throws AlreadyBoundException, IOException {
        // TODO better command parsing
        Configuration config;
        if (args.length > 1 && args[0].equals("-config"))
            config = Configuration.load(new File(args[1]));
        else
            config = Configuration.load();

        // create job manager dir
        ID id = new ID(config.getId());
        File localDir = config.getLocalDir();
        File dir = new File(localDir, id.toString());
        if (!dir.isDirectory() && !dir.mkdirs())
            throw new IllegalArgumentException("Failed to create job manager directory: " + dir.toString());

        // create job manager
        String registryHost = config.getRmiRegistryHost();
        int registryPort = config.getRmiRegistryPort();
        Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
        String remoteURI = "/jobmanager/" + config.getPeerId();
        long splitSize = config.getSplitSize();
        JobManager manager = new WorkManager(id, registry, remoteURI, splitSize, dir);

        // start worker
        Worker worker = new Worker((WorkManager)manager);
        new Thread(worker).start();

        // register job manager
        int rmiPort = config.getRmiPort();
        Remote stub = UnicastRemoteObject.exportObject(manager, rmiPort);
        String uri = "/jobmanager/" + id;
        registry.rebind(uri, stub);

        System.out.println("JobManager started on port " + rmiPort);
    }

}
