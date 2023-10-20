package de.tuberlin.dima.minidb.io.manager;

import java.io.IOException;
import java.util.ArrayList;

public class WriteThread extends Thread {

    /**
     * runs as long as it is alive
     */
    private volatile boolean isAlive;

    /**
     * tells thread to shut down soon. It will save everything to the disk and then shut down
     */
    private volatile boolean shutdown;

    /**
     * reference to the BufferPoolManager that creates this object
     */
    private final BufferPoolManagerImpl bufferPoolManager;

    /**
     * constructor
     * @param bufferPoolManager that creates this object
     */
    public WriteThread (BufferPoolManagerImpl bufferPoolManager) {
        this.isAlive = true;
        this.shutdown = false;
        this.bufferPoolManager = bufferPoolManager;
    }

    /**
     * set isAlive to false in the run method ASAP.
     */
    public void shutdown(){
        shutdown = true;
    }


    /**
     * Writes the first SubQueue of the writeQueue to the disk and releases the corresponding ioBuffer.
     * If an WriteQueueEntry of that subQueue is in the LoadQueue this thread does nothing for this entry
     * and the ReadThread handles it.
     */
    @Override
    public void run () {
        while(isAlive){

            // tries to set isAlive to false if shutdown is set. Can only be done once it wrote the whole queue to the disk
            if (shutdown && !moreEntriesToWrite(bufferPoolManager.writeQueueEntries)) isAlive = false;

            // do nothing until there is something in the queue
            if (bufferPoolManager.writeQueueEntries.isEmpty()) continue;

            ArrayList<WriteQueueEntry> writeRequests;
            synchronized (bufferPoolManager.writeQueueEntries){ //probably fine, if it goes in it goes out
                //since it waited on the monitor another thread might have already deleted the only element in the queue - UNSURE ABOUT THIS!!
                if (bufferPoolManager.writeQueueEntries.isEmpty()) continue;

                //gets the subQueue and deletes it from the Queue
                writeRequests = bufferPoolManager.writeQueueEntries.get(0);
                bufferPoolManager.writeQueueEntries.remove(0);
            }

            ResourceManager rm = writeRequests.get(0).getResourceManager();

            synchronized (BufferPoolManagerImpl.readWriteThreadMonitor) { //nested sync, but only changeBufferAvailability sync inside, so prob ok
                //System.out.println("WriteThread will do something");
                try {
                    for (WriteQueueEntry writeQueueEntry: writeRequests){
                        if (!writeQueueEntry.isSetForImmediateRefetch()) { // page would be in the loadQueue and would go directly into the cache again
                            synchronized (rm){
                                rm.writePageToResource(writeQueueEntry.getBufferToWrite(), writeQueueEntry.getPage());
                            }

                            //as the page was written to the disk, it can act as a new ioBuffer
                            bufferPoolManager.syncAddToFreeIOBuffers(rm.getPageSize(), writeQueueEntry.getBufferToWrite());
                            //System.out.println("WriteThread added free buffer");
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        System.out.println("WRITE THREAD NOT ALIVE ANYMORE");
    }

    /**
     * checks if writeQueue is empty
     * @param writeQueue entries that are supposed to be written
     * @return false if empty, true otherwise
     */
    private boolean moreEntriesToWrite(ArrayList<ArrayList<WriteQueueEntry>> writeQueue){
        if (writeQueue.isEmpty()) return false;
        for (ArrayList<WriteQueueEntry> subQueue : writeQueue){
            if (!subQueue.isEmpty()) return true;
        }
        return false;
    }
}
