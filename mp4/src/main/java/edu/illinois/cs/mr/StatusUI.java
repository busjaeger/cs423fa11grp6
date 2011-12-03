package edu.illinois.cs.mr;

import java.awt.event.ActionEvent;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.Enumeration;

import javax.imageio.ImageIO;
import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.Timer;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;
import javax.swing.tree.TreeSelectionModel;

import edu.illinois.cs.mr.Node.NodeServices;
import edu.illinois.cs.mr.jm.AttemptStatus;
import edu.illinois.cs.mr.jm.JobID;
import edu.illinois.cs.mr.jm.JobStatus;
import edu.illinois.cs.mr.jm.Phase;
import edu.illinois.cs.mr.jm.TaskStatus;
import edu.illinois.cs.mr.util.ImmutableStatus;
import edu.illinois.cs.mr.util.RPC;

public class StatusUI extends JFrame implements TreeSelectionListener {

    private static final long serialVersionUID = 6732880434775381175L;
    
    private JLabel treeLabel, labelID, labelState, labelDetail, labelMessage, 
                    labelTotal, labelWaiting, labelRunning;
    private JTree tree;
    private DefaultMutableTreeNode root;
    private int nodeId;
    private DefaultTreeModel treeModel;
    static NodeServices services;
    
    @SuppressWarnings("rawtypes")
    private void BuildTree() {
        JobID[] jobIds = null;
        try {
            jobIds = services.getJobIDs();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if(jobIds != null) {
            for(int i=0; i<jobIds.length; i++)
            {
                boolean jobfound = false;
                DefaultMutableTreeNode jobnode = null;
                for (Enumeration e = root.children(); e.hasMoreElements() ;) {
                    jobnode = (DefaultMutableTreeNode) e.nextElement();
                    int nid = Integer.parseInt(((String)jobnode.getUserObject()).split(" ")[1]);
                    if(jobIds[i].getValue() == nid) {
                        jobfound = true;
                        break;
                    }
                }
                if(!jobfound)
                {
                    jobnode = new DefaultMutableTreeNode("Job " + jobIds[i].getValue());
                    treeModel.insertNodeInto(jobnode, root, root.getChildCount());
                }
                
                try {
                    JobStatus job = services.getJobStatus(jobIds[i]);
                    
                    for (Phase phase : Phase.values())
                    {
                        DefaultMutableTreeNode phasenode = null;
                        boolean phasefound = false;
                        for (Enumeration e = jobnode.children(); e.hasMoreElements() ;) {
                            phasenode = (DefaultMutableTreeNode) e.nextElement();
                            String phz = ((String)phasenode.getUserObject()).split(" ")[1];
                            if(phz.equals(phase.name())) {
                                phasefound = true;
                                break;
                            }
                        }
                        if(!phasefound) {
                            phasenode = new DefaultMutableTreeNode("Phase " + phase.name());
                            treeModel.insertNodeInto(phasenode, jobnode, jobnode.getChildCount());
                        }
                        
                        for (TaskStatus task : job.getTaskStatuses(phase)) {
                            DefaultMutableTreeNode tasknode = null;
                            boolean taskfound = false;
                            for (Enumeration e = phasenode.children(); e.hasMoreElements() ;) {
                                tasknode = (DefaultMutableTreeNode) e.nextElement();
                                int tid = Integer.parseInt(((String)tasknode.getUserObject()).split(" ")[1]);
                                if(task.getId().getValue() == tid) {
                                    taskfound = true;
                                    break;
                                }
                            }
                            if(!taskfound)
                            {
                                tasknode = new DefaultMutableTreeNode("Task " + task.getId().getValue());
                                treeModel.insertNodeInto(tasknode, phasenode, phasenode.getChildCount());
                            }
                            
                            for (AttemptStatus attempt : task.getAttemptStatuses()) {
                                DefaultMutableTreeNode attemptnode = null;
                                boolean attemptfound = false;
                                for (Enumeration e = tasknode.children(); e.hasMoreElements() ;) {
                                    attemptnode = (DefaultMutableTreeNode) e.nextElement();
                                    int aid = Integer.parseInt(((String)attemptnode.getUserObject()).split(" ")[1]);
                                    if(attempt.getId().getValue() == aid) {
                                        attemptfound = true;
                                        break;
                                    }
                                }
                                if(!attemptfound)
                                {
                                    attemptnode = new DefaultMutableTreeNode("Attempt " + attempt.getId().getValue());
                                    treeModel.insertNodeInto(attemptnode, tasknode, tasknode.getChildCount());
                                }
                            }
                        }
                    }
                } catch(IOException e) { }
            }
        }
    }
    
    private String GetTotalTime(ImmutableStatus<?> status) {
        String ret = "n/a";
        if (status.isDone()) {
            long created = status.getCreatedTime();
            long done = status.getDoneTime();
            long total = done - created;
            ret = String.valueOf(total);
        }
        return ret;
    }
    
    private String GetWaitTime(ImmutableStatus<?> status) {
        String ret = "n/a";
        if (status.isDone()) {
            long waiting = status.getBeginWaitingTime();
            long running = status.getBeginRunningTime();

            // we may not know each event, due to our heart beat frequency
            // neither known
            if (waiting != -1 && running != -1) {
                ret = String.valueOf(running - waiting);
            }
        }
        return ret;
    }
    
    private String GetRunTime(ImmutableStatus<?> status) {
        String ret = "n/a";
        if (status.isDone()) {
            long running = status.getBeginRunningTime();
            long done = status.getDoneTime();

            // running time known
            if (running != -1) {
                ret = String.valueOf(done - running);
            }
        }
        return ret;
    }
    
    private void DrawSelectedData(DefaultMutableTreeNode node) {
        String title = (String) node.getUserObject();
        
        String type = title.split(" ")[0];
        String id = title.split(" ")[1];
        
        labelID.setText(type + " ID: \t" + id);
        labelID.setVisible(true);
        
        try {
            if(type.startsWith("Job")) {
                JobStatus js = services.getJobStatus(JobID.fromQualifiedString(nodeId + "-" + id));
                labelState.setText("State: \t" + js.getState());
                labelState.setVisible(true);
                labelDetail.setText("Phase: \t" + js.getPhase());
                labelDetail.setVisible(true);
                if(js.isDone()) {
                    labelTotal.setText("Total Time: \t" + GetTotalTime(js));
                    labelTotal.setVisible(true);
                    labelWaiting.setText("Waiting Time: \t" + GetWaitTime(js));
                    labelWaiting.setVisible(true);
                    labelRunning.setText("Running Time: \t" + GetRunTime(js));
                    labelRunning.setVisible(true);
                } else {
                    labelTotal.setVisible(false);
                    labelWaiting.setVisible(false);
                    labelRunning.setVisible(false);
                }
                labelMessage.setVisible(false);
                
            }
            else if(type.startsWith("Phase")) {
                DefaultMutableTreeNode jobnode = (DefaultMutableTreeNode)node.getParent();
                String jobid = ((String)jobnode.getUserObject()).split(" ")[1];
                String phaseid = ((String)node.getUserObject()).split(" ")[1];
                JobStatus js = services.getJobStatus(JobID.fromQualifiedString(nodeId + "-" + jobid));
                ImmutableStatus<JobID> phase = js.getPhaseStatus(Phase.valueOf(phaseid));

                labelState.setText("State: \t" + phase.getState());
                labelState.setVisible(true);
                labelMessage.setVisible(false);
                labelDetail.setVisible(false);
                if(phase.isDone()) {
                    labelTotal.setText("Total Time: \t" + GetTotalTime(phase));
                    labelTotal.setVisible(true);
                    labelWaiting.setText("Waiting Time: \t" + GetWaitTime(phase));
                    labelWaiting.setVisible(true);
                    labelRunning.setText("Running Time: \t" + GetRunTime(phase));
                    labelRunning.setVisible(true);
                } else {
                    labelTotal.setVisible(false);
                    labelWaiting.setVisible(false);
                    labelRunning.setVisible(false);
                }
            }
            else if(type.startsWith("Task")) {
                DefaultMutableTreeNode phasenode = (DefaultMutableTreeNode)node.getParent();
                DefaultMutableTreeNode jobnode = (DefaultMutableTreeNode)phasenode.getParent();
                String jobid = ((String)jobnode.getUserObject()).split(" ")[1];
                String phaseid = ((String)phasenode.getUserObject()).split(" ")[1];
                JobStatus js = services.getJobStatus(JobID.fromQualifiedString(nodeId + "-" + jobid));
                for (TaskStatus task : js.getTaskStatuses(Phase.valueOf(phaseid))) {
                    if(task.getId().getValue() == Integer.parseInt(id)) {
                        labelState.setText("State: \t" + task.getState());
                        labelState.setVisible(true);
                        labelMessage.setVisible(false);
                        labelDetail.setVisible(false);
                        if(task.isDone()) {
                            labelTotal.setText("Total Time: \t" + GetTotalTime(task));
                            labelTotal.setVisible(true);
                            labelWaiting.setText("Waiting Time: \t" + GetWaitTime(task));
                            labelWaiting.setVisible(true);
                            labelRunning.setText("Running Time: \t" + GetRunTime(task));
                            labelRunning.setVisible(true);
                        } else {
                            labelTotal.setVisible(false);
                            labelWaiting.setVisible(false);
                            labelRunning.setVisible(false);
                        }
                        break;
                    }    
                }
            }
            else if(type.startsWith("Attempt")) {
                DefaultMutableTreeNode tasknode = (DefaultMutableTreeNode)node.getParent();
                String taskid = ((String)tasknode.getUserObject()).split(" ")[1];
                DefaultMutableTreeNode phasenode = (DefaultMutableTreeNode)tasknode.getParent();
                String phaseid = ((String)phasenode.getUserObject()).split(" ")[1];
                DefaultMutableTreeNode jobnode = (DefaultMutableTreeNode)phasenode.getParent();
                String jobid = ((String)jobnode.getUserObject()).split(" ")[1];
                JobStatus js = services.getJobStatus(JobID.fromQualifiedString(nodeId + "-" + jobid));
                for (TaskStatus task : js.getTaskStatuses(Phase.valueOf(phaseid))) {
                    if(task.getId().getValue() == Integer.parseInt(taskid)) {
                        for (AttemptStatus attempt : task.getAttemptStatuses()) {
                            if(attempt.getId().getValue() == Integer.parseInt(id)) {
                                labelState.setText("State: \t" + attempt.getState());
                                labelState.setVisible(true);
                                labelDetail.setText("On Node: \t" + attempt.getTargetNodeID());
                                labelDetail.setVisible(true);
                                if (attempt.getMessage() != null) {
                                    labelMessage.setText("Message: \t" + attempt.getMessage());
                                    labelMessage.setVisible(true);
                                }
                                if(attempt.isDone()) {
                                    labelTotal.setText("Total Time: \t" + GetTotalTime(attempt));
                                    labelTotal.setVisible(true);
                                    labelWaiting.setText("Waiting Time: \t" + GetWaitTime(attempt));
                                    labelWaiting.setVisible(true);
                                    labelRunning.setText("Running Time: \t" + GetRunTime(attempt));
                                    labelRunning.setVisible(true);
                                } else {
                                    labelTotal.setVisible(false);
                                    labelWaiting.setVisible(false);
                                    labelRunning.setVisible(false);
                                }
                                return;
                            }
                        }
                    }    
                }
            }
        } catch(IOException ioe) { }
    }

    public static void main(String[] args) {
        String host;
        int port;
        if (args.length > 2 && args[1].equals("-n")) {
            String[] target = args[2].split(":");
            host = target[0];
            port = Integer.parseInt(target[1]);
        } else {
            host = "localhost";
            port = 60001;
        }
        try {
            services = RPC.newClient(host, port, NodeServices.class);
            } catch(IOException ioe) {
                System.out.println("Error connecting to node.\n");
                System.exit(2);
            } 
        
        StatusUI frame = new StatusUI();
        
        frame.setVisible(true);
    }

    public StatusUI() {
        SetupFrame();
        SetupGUIElements();
        BuildTree();
        
        Action updateData = new AbstractAction() {
            private static final long serialVersionUID = -2391347399892015494L;
            public void actionPerformed(ActionEvent e) {
                BuildTree();
                TreePath stp = tree.getSelectionPath();
                if(stp!=null) {
                    DefaultMutableTreeNode selectedNode = (DefaultMutableTreeNode)(stp.getLastPathComponent());
                    DrawSelectedData(selectedNode);
                }
            }
        };
        
        new Timer(1000, updateData).start();
    }
  
    private void SetupFrame() {
        this.setSize(800, 600);
        this.setLocation(7, 7);
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.setTitle("MP4 Status Console");
        this.setResizable(false);
        
        try {
            BufferedImage icon = ImageIO.read(this.getClass().getResource("icon.png"));
            this.setIconImage(icon);
        } catch (Exception e) { }
    }
  
    private void SetupGUIElements() {
        JPanel pane = new JPanel(null);
        
        treeLabel = new JLabel("Jobs Created on this Node:");
        treeLabel.setLocation(12, 10);
        treeLabel.setSize(300,20);
        pane.add(treeLabel);
        
        nodeId = services.getNodeID().getValue();
        
        root = new DefaultMutableTreeNode("Node "+nodeId);
        treeModel = new DefaultTreeModel(root);
        tree = new JTree(treeModel);
        tree.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
        tree.addTreeSelectionListener(this);
        tree.setShowsRootHandles(true);
        JScrollPane scrollPane = new JScrollPane(tree, 
            JScrollPane.VERTICAL_SCROLLBAR_ALWAYS,
            JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        scrollPane.setLocation(10, 35);
        scrollPane.setSize(300, 525);
        pane.add(scrollPane);
        
        labelID = new JLabel("ID: \t");
        labelID.setSize(300, 20);
        labelID.setLocation(350, 40);
        labelID.setVisible(false);
        pane.add(labelID);
        
        labelState = new JLabel("State: \t");
        labelState.setSize(300, 20);
        labelState.setLocation(350, 80);
        labelState.setVisible(false);
        pane.add(labelState);
        
        labelDetail = new JLabel(": \t");
        labelDetail.setSize(300, 20);
        labelDetail.setLocation(350, 120);
        labelDetail.setVisible(false);
        pane.add(labelDetail);
        
        labelMessage = new JLabel("Message: \t");
        labelMessage.setSize(300, 20);
        labelMessage.setLocation(350, 160);
        labelMessage.setVisible(false);
        pane.add(labelMessage);
        
        labelTotal = new JLabel("Total Time: \t");
        labelTotal.setSize(300, 20);
        labelTotal.setLocation(350, 200);
        labelTotal.setVisible(false);
        pane.add(labelTotal);
        
        labelWaiting = new JLabel("Waiting Time: \t");
        labelWaiting.setSize(300, 20);
        labelWaiting.setLocation(350, 240);
        labelWaiting.setVisible(false);
        pane.add(labelWaiting);
        
        labelRunning = new JLabel("Running Time: \t");
        labelRunning.setSize(300, 20);
        labelRunning.setLocation(350, 280);
        labelRunning.setVisible(false);
        pane.add(labelRunning);
        
        this.add(pane);
    }
        
    private void ClearData() {
        labelID.setVisible(false);
        labelState.setVisible(false);
        labelDetail.setVisible(false);
        labelMessage.setVisible(false);
        labelTotal.setVisible(false);
        labelWaiting.setVisible(false);
        labelRunning.setVisible(false);
    }
  
    /**
     * Returns the last path element of the selection.
     */
    public void valueChanged(TreeSelectionEvent e) {

        DefaultMutableTreeNode node = 
                (DefaultMutableTreeNode) tree.getLastSelectedPathComponent();

        if (node == null)               //Nothing is selected.     
        {
                ClearData();
                return;
        }

        if (!node.isRoot()) {
                DrawSelectedData(node);
        }
        else {
                tree.setSelectionPath(null);
                ClearData();
        }
    }
}