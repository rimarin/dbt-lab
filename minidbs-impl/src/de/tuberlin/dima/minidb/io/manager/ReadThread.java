package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.*;

import java.io.IOException;
import java.util.ArrayList;

public class ReadThread extends Thread {

    /**
     * runs as long as it is alive
     */
    private volatile boolean isAlive;

    /**
     * reference to the BufferPoolManager that creates this object
     */
    private final BufferPoolManagerImpl bufferPoolManager;

    /**
     * constructor
     * @param bufferPoolManager that creates this object
     */
    public ReadThread (BufferPoolManagerImpl bufferPoolManager) {
        this.isAlive = true;
        this.bufferPoolManager = bufferPoolManager;
    }

    /**
     * stops this thread --> all entries in the loadQueue won't be handled.
     */
    public void shutdown(){
        isAlive = false;
    }


    /**
     * Loads the first SubQueue of the writeQueue from the disk into an CacheableData object, with an ioBuffer it got
     * from the bufferPoolManager. Then it adds it to the cache and pins it some number of times.
     * If a request of the SubQueue also in the writeQueue it takes the CacheableData directly from there instead of
     * using the resource manager to load from the disk
     */
    @Override
    public void run () {
        while(isAlive){
            boolean deletedFromQueue = false;
            // do nothing until there is something in the loadQueue
            if (bufferPoolManager.loadQueueEntries.isEmpty()) continue;

            ArrayList<LoadQueueEntry> requests;
            synchronized (bufferPoolManager.loadQueueEntries) { //think fine; if it goes in it goes out
                //since it waited on the monitor another thread might have already deleted the only element in the queue - UNSURE ABOUT THIS!!
                if (bufferPoolManager.loadQueueEntries.isEmpty()) continue;

                //gets the subQueue and deletes it from the Queue
                requests = bufferPoolManager.loadQueueEntries.get(0);
            }

            ResourceManager rm = requests.get(0).getResourceManager();

            requestsInWriteQueue(requests); //TODO include again
            if (requests.isEmpty()) {
                //synchronized (bufferPoolManager.loadQueueEntries){
                    bufferPoolManager.loadQueueEntries.remove(0);
                //}

                continue;
            }


            byte[][] ioBuffers = bufferPoolManager.syncGetNFreeIOBuffers(requests.size(), rm.getPageSize());
            CacheableData[] cacheableDatas;

            synchronized (BufferPoolManagerImpl.readWriteThreadMonitor) { //nested synchronize
                try {
                    synchronized (rm){
                        cacheableDatas = rm.readPagesFromResource(ioBuffers, requests.get(0).getPageNumber());
                    }

                } catch (IOException e) {
                    throw new RuntimeException(e);
                    //cacheableDatas = new CacheableData[requests.size()];
                }
            }

            for (int i = 0; i < cacheableDatas.length; i++) {
                handleCacheableData(cacheableDatas[i], rm, requests.get(i));
            }

            // remove from Queue only after it got added to the cache, so it another thread ca not add the same page to the LoadQueue
            // in between deleting these requests from the LoadQueue and adding them to cache
            synchronized (bufferPoolManager.loadQueueEntries){
                bufferPoolManager.loadQueueEntries.remove(0);
            }

        }

    }

    private void handleCacheableData(CacheableData cacheableData, ResourceManager rm, LoadQueueEntry request){
        // saves the cacheableData object to the cache and pins it a number of times.
        // stores the evictedCacheEntry and handles it immediately after
        PageCache cache = bufferPoolManager.caches.get(rm.getPageSize());
        EvictedCacheEntry evictedCacheEntry = bufferPoolManager.cacheAddPage(cache, cacheableData, request.getResourceId());
        for (int i = 0; i < request.getPinning(); i++) {
            bufferPoolManager.syncCacheGetPageAndPin(cache, request.getResourceId(), request.getPageNumber()); //synchronization, I think it goes in the block and can't have a problem leaving the block -> fine
        }

        // everything is stored to the cache so the loadQueueEntry will be notified
        request.setResultPage(cacheableData);
        synchronized (request) { //only one other block synchronizes on it with a wait method -> should be fine
            request.notifyAll();
        }

        // create WriteQueueElement from evictedCacheEntry and add it to the WriteQueue.
        // Only if the evictedCacheEntry has a Wrapping page that has been modified
        if (evictedCacheEntry.getWrappingPage() != null && evictedCacheEntry.getWrappingPage().hasBeenModified()) {
            int evictedResourceId = evictedCacheEntry.getResourceID();
            int pageNumber = evictedCacheEntry.getPageNumber();
            ResourceManager rm2 = bufferPoolManager.resourceManagers.get(evictedResourceId);
            byte[] buffer = evictedCacheEntry.getBinaryPage();
            CacheableData page = evictedCacheEntry.getWrappingPage();

            WriteQueueEntry writeQueueEntry = new WriteQueueEntry(evictedResourceId, pageNumber, rm2, buffer, page);
            bufferPoolManager.syncPutInCorrectSublist(writeQueueEntry, bufferPoolManager.writeQueueEntries); //maybe fine
        }
        else {
            // as we don't need to write the page we can add the binaryPage directly as a new free IObuffer
            bufferPoolManager.syncAddToFreeIOBuffers(rm.getPageSize(), evictedCacheEntry.getBinaryPage());
        }
    }


    private boolean requestInWriteQueue(LoadQueueEntry request){

        PageSize pageSizeReq = request.getResourceManager().getPageSize();
        int pageNumReq = request.getPageNumber();

        //there are 3 places where we sync on write queue. In writeThread no problem. In putInCorrectSublistWrite, only done after this method is completely finished
        synchronized (bufferPoolManager.writeQueueEntries){ //comment above

            for (ArrayList<WriteQueueEntry> writeSubQueue : bufferPoolManager.writeQueueEntries){
                for (WriteQueueEntry writeQueueEntry: writeSubQueue){

                    //same page is supposed to be written
                    if (pageNumReq == writeQueueEntry.getPageNumber() &&
                            pageSizeReq.equals(writeQueueEntry.getResourceManager().getPageSize())) {

                        writeQueueEntry.markForImmediateRefetch(true); //write Thread will do nothing

                        handleCacheableData(writeQueueEntry.getPage(), request.getResourceManager(), request);
                        // handle directly without the resource manager reading from disk
                        /*request.setResultPage(writeQueueEntry.getPage());
                        synchronized (request){ //only one other method synchronizes on this object, with a wait block inside.
                            request.notifyAll();
                        }*/

                        return true;
                    }
                }
            }
        }
        return false;
    }



    /**
     * Checks if the page of request that is supposed to be retrieved from disk is currently in the writeQueue.
     * In this case it, instead gets the page from the writeQueue and removes this request from the requests that are
     * to be retrieved from the disk
     * @param requests the loadQueueEntries that are each supposed to load a page from the disk
     */
    private void requestsInWriteQueue(ArrayList<LoadQueueEntry> requests){
        requests.removeIf(this::requestInWriteQueue);
    }
}