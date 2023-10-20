package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.api.ExtensionFactory;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.io.cache.*;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class BufferPoolManagerImpl implements BufferPoolManager{

    /**
     * The entries that are waiting to be loaded from the disk. The entries are stored in the same sublist if they are stored closely by one another
     */
    public final ArrayList<ArrayList<LoadQueueEntry>> loadQueueEntries;

    /**
     * The entries that are waiting to be written to the disk. The entries are stored in the same sublist if they will be stored closely by one another
     */
    public final ArrayList<ArrayList<WriteQueueEntry>> writeQueueEntries;

    /**
     * Stores one cache per pageSize
     */
    public final HashMap<PageSize, PageCache> caches;

    /**
     * Stores a resource manager for each resourceId (key)
     */
    public final HashMap<Integer, ResourceManager> resourceManagers;

    /**
     * stores the ioBuffers for each pageSize
     */
    private final HashMap<PageSize, List<byte[]>> ioBuffers;

    /**
     * The thread that reads data from the secondary storage
     */
    private final ReadThread readThread;
    /**
     * The thread that writes data to the secondary storage
     */
    private final WriteThread writeThread;

    /**
     * Contains info for caches & buffers
     */
    private final Config config;

    /**
     * Might use logger at some point.
     */
    private final Logger logger;

    /**
     * flag if the BufferManager has been closed
     */
    private boolean isClosed;


    /**
     * Mock object used as a Monitor to synchronize read and write access to the disk
     */
    public static final Object readWriteThreadMonitor = new Object();

    /**
     * Decides if we add a page to a subQueue. If another page in that subQueue has a pageNumber that is within this
     * range of this page's pageNumber, we add it.
     */
    public static final int MAX_PAGE_NUMBER_DIFF_IN_SINGLE_QUEUE = 1;


    /**
     * Constructor
     */
    public BufferPoolManagerImpl (Config config, Logger logger) {
        this.config = config;
        this.logger = logger;

        // initialise collections
        loadQueueEntries = new ArrayList<>();
        writeQueueEntries = new ArrayList<>();

        caches = new HashMap<>();
        resourceManagers = new HashMap<>();
        ioBuffers = new HashMap<>();

        // initialise threads
        readThread = new ReadThread(this);
        writeThread = new WriteThread(this);

        isClosed = false;
    }

    /**
     * Starts the operation of the I/O threads in the buffer pool.
     */
    @Override
    public void startIOThreads() throws BufferPoolException {

        synchronized (readThread){
            //dumb shit
            if (readThread.isAlive()) {
                System.out.println("read thread is already alive");
                System.out.println("Printing out logger name to please intellij warning" + logger.getName());
                throw new BufferPoolException("Weird that the ioThreads are started twice, but really only there to please intellij warning");
            }
            readThread.start();
        }

        synchronized (writeThread){
            //dumb shit
            if (writeThread.isAlive()) {
                System.out.println("write thread is already alive");
                System.out.println("Printing out logger name to please intellij warning" + logger.getName());
                throw new BufferPoolException("Weird that the ioThreads are started twice, but really only there to please intellij warning");
            }
            writeThread.start();
        }

    }

    /**
     * This method closes the buffer pool. It causes the following actions:
     * <ol>
     *   <li>It prevents further requests from being processed</li>
     *   <li>Its stops the reading thread from fetching any further pages. Requests that are currently in the
     *       queues to be read are discarded.</li>
     *   <li>All methods that are currently blocked and waiting on a synchronous page request will be waken up and
     *       must throw a BufferPoolException as soon as they wake up.</li>
     *   <li>It closes all resources, meaning that it gets all their pages from the cache, and queues the modified
     *       pages to be written out.</li>
     *   <li>It dereferences the caches (setting them to null), allowing the garbage collection to
     *       reclaim the memory.</li>
     * </ol>
     */
    @Override
    public void closeBufferPool() {

        // Move modified cache pages to write queue (maybe no tests) -> skip for now!
        readThread.shutdown();

        // disregard all elements in the read queue
        loadQueueEntries.clear();

        synchronized (caches){
            for (PageSize pageSize: caches.keySet()){
                PageCache cache = caches.get(pageSize);
                int cacheCapacity = config.getCacheSize(pageSize);
                int resId = -1;
                synchronized (resourceManagers){
                    for (Map.Entry<Integer, ResourceManager> entry : resourceManagers.entrySet()){
                        if (entry.getValue().getPageSize().equals(pageSize)){
                            resId = entry.getKey();
                            break;
                        }
                    }
                }

                synchronized (cache){
                    cache.unpinAllPages();
                }

                for (int i = 0; i < cacheCapacity; i++) {
                    try {
                        createNewPageAndPin(resId);
                    } catch (BufferPoolException | IOException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
        }


        //shuts down the writeThread, which tries to write all pages to the disk first.
        writeThread.shutdown();

        // disregard all elements in the write queue after this. They are saved in the WriteThread at this point
        // writeQueueEntries.clear();

        isClosed = true;
    }

    /**
     * Registers a resource with this buffer pool. All requests for this resource are then served through this buffer pool.
     * <p>
     * If the buffer pool has already a cache for the page size used by that resource, the cache will
     * be used for the resource. Otherwise, a new cache will be created for that page size. The size
     * of the cache that is to be created is read from the <tt>Config</tt> object. In addition to the
     * cache, the buffer pool must also create the I/O buffers for that page size.
     *
     * @param id The id of the resource.
     * @param manager The ResourceManager through which pages of the resource are read from and
     *                written to secondary storage.
     *
     * @throws BufferPoolException Thrown, if the buffer pool has been closed, the resource is already
     *                             registered with another handler or a cache needs to be created but
     *                             the creation fails.
     */
    @Override
    public void registerResource(int id, ResourceManager manager) throws BufferPoolException {

        /* handle illegal request */
        if (isClosed) throw new BufferPoolException("registerResource was called but the BufferPool was already closed");

        // save resource manager
        synchronized (resourceManagers){
            if (resourceManagers.containsKey(id)) throw new BufferPoolException("Resource is already registered with another handler");
            resourceManagers.put(id, manager);
        }


        // resource manager's page size
        PageSize pageSize = manager.getPageSize();

        /*
        When a new ResourceManager is registered, you need to make sure that there is a cache for its PageSize.
        If there is no Cache for that PageSize, you must create a new one and add it to your collection.
         */
        synchronized (caches){
            if (!caches.containsKey(pageSize)){
                //if (bufferConfiguration.getNumIOBuffers() < caches.keySet().size() + 1) throw new BufferPoolException("too many caches to hold for this BufferPoolManager");
                PageCache cache = ExtensionFactory.getExtensionFactory().createPageCache(pageSize, config.getCacheSize(pageSize)); //new AdaptiveReplacementCache(pageSize, bufferConfiguration.getCacheSize(pageSize));
                caches.put(pageSize, cache);
            }
        }


        /*
        when a new ResourceManager is registered, you must ensure a list of free buffers for its PageSize.
        If there is no list of free buffers for that PageSize, you must create a new one and add it to your collection.
        don’t forget to also create the buffers! After creating the list, you need to create config.getNumIOBuffers() empty buffers,
        where each buffer has the length of the number of bytes requires to store a page, i.e., byte[pageSize.getNumberOfBytes()].
         */
        synchronized (ioBuffers){
            if (!ioBuffers.containsKey(pageSize)) {
                // check if number of caches is exceeded (!!!Probably not correct getNumOfIOBuffers describes the number of ioBuffer for each pageSize and not the max number of caches)
                // if (bufferConfiguration.getNumIOBuffers() < ioBuffers.keySet().size() + 1) throw new BufferPoolException("too many ioBuffers to hold for this BufferPoolManager");

                // list of empty buffers
                ArrayList<byte[]> freeBuffers = new ArrayList<>();

                // create new empty buffers & add to buffer list
                for (int i = 0; i < config.getNumIOBuffers(); i++)
                    freeBuffers.add(new byte[pageSize.getNumberOfBytes()]);

                // add buffer list to buffer's hashmap
                ioBuffers.put(pageSize, freeBuffers);
            }
        }

    }

    /**
     * Transparently fetches the page defined by the page number for the given resource.
     * If the cache contains the page, it will be fetched from there, otherwise it will
     * be loaded from secondary storage. The page is pinned in order to prevent eviction from
     * the cache.
     * <p>
     * The binary page will be wrapped by a page object depending on the resource type.
     * If the resource type is for example a table, then the returned object is a <code>TablePage</code>.
     *
     * @param resourceId The id of the resource.
     * @param pageNumber The page number of the page to fetch.
     * @return The requested page, wrapped by a structure to make it accessible.
     *
     * @throws BufferPoolException Thrown, if the given resource is not registered at the buffer pool,
     *                             the buffer pool is closed, or an internal problem occurred.
     * @throws IOException Thrown, if the page had to be loaded from secondary storage and the loading
     *                     failed due to an I/O problem.
     */
    @Override
    public CacheableData getPageAndPin(int resourceId, int pageNumber) throws BufferPoolException, IOException {

        if (isClosed) throw new BufferPoolException("getPageAndPin was called but the BufferPool was already closed");

        // First check try to get page from cache and pin it
        ResourceManager rm = resourceManagers.get(resourceId);
        CacheableData requestedPage = syncCacheGetPageAndPin(caches.get(rm.getPageSize()), resourceId, pageNumber); //like other cache syncs

        // Page was in cache and therefore also got pinned and registered a hit
        if (requestedPage != null){
            return requestedPage;
        }

        // If pages does not exist in the cache (cache miss), put it in the LoadQueue and wait until it gets loaded. After it got loaded return the resulting CacheableData.
        else {

            LoadQueueEntry loadQueueEntry = new LoadQueueEntry(resourceId, pageNumber, rm, caches.get(rm.getPageSize()) ,true);
            boolean alreadyInQueue = false;

            LoadQueueEntry possibleSimilarRequest = syncAlreadyInLoadQueue(loadQueueEntry); //goes in and out
            if (possibleSimilarRequest != null){
                alreadyInQueue = true;
                loadQueueEntry = possibleSimilarRequest; //this will make the method wait on the same LoadQueueEntry that is in the LoadQueue
                loadQueueEntry.increasePinning();
            }

            if (!alreadyInQueue) syncPutInCorrectSublist(loadQueueEntry, loadQueueEntries); //goes in and out

            try {
                synchronized (loadQueueEntry){ // goes in
                    loadQueueEntry.wait(); // releases lock (we think)
                }

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (loadQueueEntry.getResultPage() == null) throw new IOException("the result page for this loadQueueElement was set to null, instead of an actual page");

            return loadQueueEntry.getResultPage();
        }
    }

    /**
     * Unpins a given page and in addition fetches another page from the same resource. This method works exactly
     * like the method {@link de.tuberlin.dima.minidb.io.manager.BufferPoolManager#getPageAndPin(int, int)}, only that it
     * unpins a page before beginning the request for the new page.
     * <p>
     * Similar to the behavior of {@link de.tuberlin.dima.minidb.io.manager.BufferPoolManager#unpinPage(int, int)}, no exception
     * should be thrown, if the page to be unpinned was already unpinned or no longer in the cache.
     * <p>
     * This method should perform better than isolated calls to unpin a page and getting a new one.
     * It should, for example, only acquire the lock/monitor on the cache once, instead of twice.
     *
     * @param resourceId The id of the resource.
     * @param unpinPageNumber The page number of the page to be unpinned.
     * @param getPageNumber The page number of the page to get and pin.
     * @return The requested page, wrapped by a structure to make it accessible.
     *
     * @throws BufferPoolException Thrown, if the given resource is not registered at the buffer pool,
     *                             the buffer pool is closed, or an internal problem occurred.
     * @throws IOException Thrown, if the page had to be loaded from secondary storage and the loading
     *                     failed due to an I/O problem.
     */
    @Override
    public CacheableData unpinAndGetPageAndPin(int resourceId, int unpinPageNumber, int getPageNumber) throws BufferPoolException, IOException {

        if (isClosed) throw new BufferPoolException("unpinAndGetPageAndPin was called but the BufferPool was already closed");

        //syncs handled in respective methods
        unpinPage(resourceId, unpinPageNumber);
        return getPageAndPin(resourceId, getPageNumber);
    }

    /**
     * Unpins a page so that it can again be evicted from the cache. This method works after the principle of
     * <i>best effort</i>: It will try to unpin the page, but will not throw any exception if the page is not
     * contained in the cache or if it is not pinned.
     *
     * @param resourceId The id of the resource.
     * @param pageNumber The page number of the page to unpin.
     */
    @Override
    public void unpinPage(int resourceId, int pageNumber) {
        ResourceManager rm = resourceManagers.get(resourceId);

        PageCache cache = caches.get(resourceManagers.get(resourceId).getPageSize());

        synchronized (cache){ //like the other cache syncs
            //CacheableData cacheableData = cache.getPage(resourceId, pageNumber);

            //for (int i = 0; i < 10; i++) {
                cache.unpinPage(resourceId, pageNumber);
            //}

        }


    }

    /**
     * Prefetches a page. If the page is currently in the cache, this method causes a hit on the page.
     * If not, an asynchronous request to load the page is issued. When the asynchronous request has loaded the
     * page, it adds it to the cache without causing it to be considered hit.
     * <p>
     * The rationale behind that behavior is the following: Pages that are already in the cache are hit again and
     * become frequent, which is desirable, since it is in fact accessed multiple times. (A prefetch is typically
     * followed by a getAndPin request short after). Any not yet contained page is added to the cache and not hit,
     * so that the later getAndPin request is in fact the first request and keeps the page among the recent items
     * rather than among the frequent.
     * <p>
     * This function returns immediately and does not wait for any I/O operation to complete.
     *
     * @param resourceId The id of the resource.
     * @param pageNumber The page number of the page to fetch.
     * @throws BufferPoolException If the buffer pool is closed, or the resource is not registered.
     */
    @Override
    public void prefetchPage(int resourceId, int pageNumber) throws BufferPoolException {
        if (isClosed) throw new BufferPoolException("prefetchPage was called but the BufferPool was already closed");
        if (!resourceManagers.containsKey(resourceId)) throw new BufferPoolException("The resource is not registered");

        ResourceManager rm = resourceManagers.get(resourceId);
        PageCache cache = caches.get(rm.getPageSize());

        CacheableData page;
        //cause cache hit if it is in cache
        synchronized (cache){ //like the other cache syncs, needed to register both hits
             page = cache.getPage(resourceId, pageNumber);
        }

        if (page == null){      // page not in cache -> put it in loadQueue, but without pinning
            LoadQueueEntry loadQueueEntry = new LoadQueueEntry(resourceId, pageNumber, rm, cache, false);

            LoadQueueEntry possibleSimilarRequest = syncAlreadyInLoadQueue(loadQueueEntry);

            if (possibleSimilarRequest == null) syncPutInCorrectSublist(loadQueueEntry, loadQueueEntries);
        }
        // This function returns immediately and does not wait for any I/O operation to complete.
        // ==> don't do anything after adding the item to the loadQueueEntry
    }

    /**
     * Prefetches a sequence of pages. Behaves exactly like
     * {@link de.tuberlin.dima.minidb.io.manager.BufferPoolManager#prefetchPage(int, int)}, only that it prefetches
     * multiple pages.
     *
     * @param resourceId The id of the resource.
     * @param startPageNumber The page number of the first page to prefetch.
     * @param endPageNumber The page number of the last page to prefetch.
     * @throws BufferPoolException If the buffer pool is closed, or the resource is not registered.
     */
    @Override
    public void prefetchPages(int resourceId, int startPageNumber, int endPageNumber) throws BufferPoolException {
        if (isClosed) throw new BufferPoolException("prefetchPages was called but the BufferPool was already closed");
        if (!resourceManagers.containsKey(resourceId)) throw new BufferPoolException("The resource is not registered");
        for(int i = startPageNumber; i <= endPageNumber; i++){
            prefetchPage(resourceId, i);
        }
    }

    /**
     * Creates a new empty page for the resource described by the id and pins the new page. This method is called
     * whenever during the insertion into a table a new page is needed, or index insertion requires an additional
     * page.
     * <p>
     * The method must take a buffer (from the I/O buffers) and call the ResourceManager
     * {@link de.tuberlin.dima.minidb.io.manager.ResourceManager#reserveNewPage(byte[])} to create a new page inside
     * that buffer. The resource manager will also reserve and assign a new page number for that page and
     * create the wrapping page (e.g. TablePage or IndexPage).
     *
     * @param resourceId The id of the resource for which the page is to be created.
     * @return A new empty page that is part of the given resource.
     * @throws BufferPoolException Thrown, if the given resource is not registered at the buffer pool,
     *                             the buffer pool is closed, or an internal problem occurred.
     * @throws IOException Thrown, if the page had to be loaded from secondary storage and the loading
     *                     failed due to an I/O problem.
     */
    @Override
    public CacheableData createNewPageAndPin(int resourceId) throws BufferPoolException, IOException {
        return createNewPageAndPin(resourceId, null);
    }

    /**
     * Creates a new empty page for the resource described by the id and pins the new page. This method is called
     * whenever during the insertion into a table a new page is needed, or index insertion requires an additional
     * page. See {@link de.tuberlin.dima.minidb.io.manager.BufferPoolManager#createNewPageAndPin(int)} for details.
     * <p>
     * This method takes a parameter that lets the caller specify the type of page to be created. That is important
     * for example for indexes, which have multiple types of pages (leaf pages, inner node pages). The parameter need
     * not be interpreted by the buffer pool manager, but it may rather be passed to the <tt>ResourceManager</tt> of the
     * given resource, who will interpret it and make sure that the correct page type is instantiated.
     *
     * @param type The resource type, e.g. table or index.
     * @param resourceId The id of the resource for which the page is to be created.
     * @return A new empty page that is part of the given resource.
     * @throws BufferPoolException Thrown, if the given resource is not registered at the buffer pool,
     *                             the buffer pool is closed, or an internal problem occurred.
     * @throws IOException Thrown, if the page had to be loaded from secondary storage and the loading
     *                     failed due to an I/O problem.
     */
    @Override
    public CacheableData createNewPageAndPin(int resourceId, Enum<?> type) throws BufferPoolException, IOException {

        if (isClosed) throw new BufferPoolException("createNewPageAndPin was called but the BufferPool was already closed");
        if (!resourceManagers.containsKey(resourceId)) throw new BufferPoolException("The resource is not registered");

        // cache, resource manager, and free buffer references
        ResourceManager rm = resourceManagers.get(resourceId);
        byte[] freeBuffer = syncGetFreeIOBuffers(rm.getPageSize());
        PageCache cache = caches.get(rm.getPageSize());

        // create a new page inside the buffer, add & pin it to the cache
        CacheableData newPage;
        try {
            synchronized (rm){
                if (type == null){
                    newPage = rm.reserveNewPage(freeBuffer);
                    //newPage = rm.reserveNewPage(new byte[rm.getPageSize().getNumberOfBytes()]);
                } else {
                    newPage = rm.reserveNewPage(freeBuffer, type);
                    //newPage = rm.reserveNewPage(new byte[rm.getPageSize().getNumberOfBytes()], type);
                }
            }


            EvictedCacheEntry evictedCacheEntry;
            synchronized (cache) { //if it is inside this block, it will always leave it again, as it does not depend on any other sync -> should be fine
                evictedCacheEntry = cache.addPageAndPin(newPage, resourceId);
            }

            if (evictedCacheEntry.getWrappingPage() != null && evictedCacheEntry.getWrappingPage().hasBeenModified()) {
                int evictedResourceId = evictedCacheEntry.getResourceID();
                int pageNum = evictedCacheEntry.getPageNumber();
                ResourceManager rm2 = resourceManagers.get(evictedResourceId);
                byte[] buffer = evictedCacheEntry.getBinaryPage();
                CacheableData page = evictedCacheEntry.getWrappingPage();
                WriteQueueEntry writeQueueEntry = new WriteQueueEntry(evictedResourceId, pageNum, rm2, buffer, page);
                syncPutInCorrectSublist(writeQueueEntry, writeQueueEntries);
            }
            else {
                // as we don't need to write the page we can add the binaryPage directly as a new free IObuffer
                syncAddToFreeIOBuffers(rm.getPageSize(), evictedCacheEntry.getBinaryPage());
            }

        } catch (PageFormatException | DuplicateCacheEntryException | CachePinnedException e) {
            throw new BufferPoolException("internal problem");
        }

        return newPage;
    }

    /**
     * Gets an ioBuffer from the ioBuffers of this pageSize.
     * If it does not get one immediately it waits until one is available. (waits 20ms after each try)
     * If it gets one, it deletes this ioBuffer from the list from the ioBuffers.
     * @param pageSize -- key for buffer list
     * @return         -- the free buffer byte array
     */
    public byte[] syncGetFreeIOBuffers(PageSize pageSize) {

        synchronized (ioBuffers){

            if (!ioBuffers.get(pageSize).isEmpty()) {
                //System.out.println("buffer available = " + ioBuffers.get(pageSize).size() + " | There are " + writeQueueEntries.size() + " Elements in the WriteQueue\");");
                return ioBuffers.get(pageSize).remove(0);
            }
            //System.out.println("there was no available buffer | There are " + writeQueueEntries.size() + " Elements in the WriteQueue");
        }

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return syncGetFreeIOBuffers(pageSize);
    }


    /**
     * adds a byte[](newIOBuffer) to the ioBuffers for the pageSize. The newIOBuffer comes from an evictedPageEntry that has either
     * already been written back to the disk or which was never modified
     * @param pageSize the pageSize
     * @param newIOBuffer old binaryPage of an evictedCacheEntry
     */
    public void syncAddToFreeIOBuffers(PageSize pageSize, byte[] newIOBuffer) {
        synchronized (ioBuffers){
            ioBuffers.get(pageSize).add(newIOBuffer);
        }
    }



    /**
     * Adds a queueEntry to the specified Queue
     * Checks if there is are subQueues with the same resource manager in the queue. For these it does further checks.
     * Checks if there is any subQueue in the queue that has a pageNumber close to the new queueEntry.
     * If it contains a pageNumber that is within a range of +-1 of the new pageNumber, the queueEntry gets added to that subQueue.
     * If there is no such subQueue a new subQueue with only that element gets created and added to the WriteQueue
     * @param queueEntry the entry that gets added to the Queue
     * @param queueEntries either the LoadQueue or the WriteQueue
     */
    public <T extends QueueEntry> void syncPutInCorrectSublist(T queueEntry, ArrayList<ArrayList<T>> queueEntries) {
        ResourceManager rm = queueEntry.getResourceManager();
        int pageNumber = queueEntry.getPageNumber();
        boolean elementAdded = false;

        synchronized (queueEntries) { //in my mind goes in and goes out. not depended on anything else
            /*for (ArrayList<T> subQueue : queueEntries) {

                if (subQueue.size() >= MAX_PAGE_REQUESTS_IN_SINGLE_QUEUE) continue;
                if (rm != subQueue.get(0).getResourceManager()) continue;
                if (rm.getPageSize().getNumberOfBytes() != subQueue.get(0).getResourceManager().getPageSize().getNumberOfBytes()) continue;

                subQueue.sort(Comparator.comparingInt(QueueEntry::getPageNumber));

                int minPageNumber = subQueue.get(0).getPageNumber();
                int maxPageNumber = subQueue.get(subQueue.size() - 1).getPageNumber();
                if (pageNumber > minPageNumber - MAX_PAGE_NUMBER_DIFF_IN_SINGLE_QUEUE && pageNumber < maxPageNumber + MAX_PAGE_NUMBER_DIFF_IN_SINGLE_QUEUE) {
                    subQueue.add(queueEntry);

                    subQueue.sort(Comparator.comparingInt(QueueEntry::getPageNumber));
                    elementAdded = true;
                }
            }*/

            if (!elementAdded){
                ArrayList<T> subQueue = new ArrayList<>();
                subQueue.add(queueEntry);
                queueEntries.add(subQueue);
            }
        }
    }

    /**
     * gets amount free ioBuffers. If there are less available this method waits until enough are available
     * @param amount of ioBuffers
     * @param pageSize the pageSize for which to get ioBuffers
     * @return byte[][] that contains amount of ioBuffers (byte[])
     */
    public byte[][] syncGetNFreeIOBuffers(int amount, PageSize pageSize){
        synchronized (ioBuffers){
            byte[][] res = new byte[amount][pageSize.getNumberOfBytes()];

            for (int i = 0; i < amount; i++) {
                res[i] = syncGetFreeIOBuffers(pageSize);
            }

            return res;
        }

    }

    /**
     * checks if an element with the same properties is already in the LoadQueue.
     * If this is the case it returns the element that is in the queue, that has the same properties.
     * Not needed for writeQueue as each element to the writeQueue is from the cache so there are no duplicates
     * @param loadQueueEntry the entry to check
     * @return the entry that is already in the loadQueue that has the same properties as the entry to check
     */
    private LoadQueueEntry syncAlreadyInLoadQueue(LoadQueueEntry loadQueueEntry){
        // test whether a similar load queue entry is already in the load queue
        // if yes, find _that_ load queue entry and wait on it, too

        synchronized (loadQueueEntries) { //in my mind goes in and goes out. not depended on anything else
            for (ArrayList<LoadQueueEntry> loadSubQueue : loadQueueEntries){
                for (LoadQueueEntry loadQueueEntryInList: loadSubQueue){
                        if (loadQueueEntry.getResourceId().intValue() == loadQueueEntryInList.getResourceId().intValue() &&
                                loadQueueEntry.getPageNumber() == loadQueueEntryInList.getPageNumber() &&
                                //both entries have a reference to a cache object from the cache hashmap of this bufferPoolManager -> check if it is the same
                                loadQueueEntry.getTargetCache() == (loadQueueEntryInList.getTargetCache()))
                        {
                            return loadQueueEntryInList;
                        }
                }
            }
        }
        return null;
    }

    public EvictedCacheEntry cacheAddPage(PageCache pageCache, CacheableData cacheableData, int resourceId){
        synchronized (pageCache){ //if it is inside this block, it will always leave it again, as it does not depend on any other sync -> should be fine
            try {
                return pageCache.addPage(cacheableData, resourceId);
            } catch (CachePinnedException | DuplicateCacheEntryException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public CacheableData syncCacheGetPageAndPin(PageCache pageCache, int resourceId, int pageNumber){
        synchronized (pageCache){ //if it is inside this block, it will always leave it again, as it does not depend on any other sync -> should be fine
            return pageCache.getPageAndPin(resourceId, pageNumber);
        }
    }





}