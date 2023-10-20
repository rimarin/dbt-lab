package de.tuberlin.dima.minidb.io.cache;

import java.util.*;

/**
 * The specification of a page cache holding a fix number of entries and supporting
 * pinning of pages.
 */
public class PageCacheImpl implements PageCache {

    /**
     * The size of the page cache in bytes.
     */
    private final PageSize PAGE_SIZE;

    /**
     * The number of pages contained in the page cache
     */
    private final int NUM_PAGES;


    /**
     * The CacheableData objects that are in this cache
     */
    private final CacheableData[] pages;

    /**
     * Information if a place of the CacheableData array is empty (page got removed, or there was never a page added).
     * CacheableData array is empty at position index, if emptyPages[index] = true
     */
    private final boolean[] emptyPages;


    /**
     * A parameter P that decides from which list we remove a page.
     */
    private int parameterP;

    /**
     * Global boolean to know if we should adjust p in a case 2 of addPage()
     * => No adjustment if we had to remove from the wrong top list, because of pinning
     */
    private boolean pAdjustmentEligible;

    /**
     * The Recent entries, which have been accessed only once in the past.
     */
    private final LinkedList<EntryId> topRecentEntriesT1;

    /**
     * Frequent entries, which have been accessed at least twice in the past.
     */
    private final LinkedList<EntryId> topFrequentEntriesT2;

    /**
     * Entries which have been evicted from T1 but are still tracked.
     */
    private final LinkedList<EntryId> bottomRecentEntriesB1;

    /**
     * Entries which have been evicted from T2 but are still tracked.
     */
    private final LinkedList<EntryId> bottomFrequentEntriesB2;

    /**
     * All our lists (T1, T2, B1, B2)
     */
    private final ArrayList<LinkedList<EntryId>> lists;

    /**
     * Initializes a new Cache with the given pageSize and numPages.
     * Init an array with numPages CacheableData objects (in the beginning these are all null)
     * Init lists t1,t2,b1,b2 and parameter p
     * @param pageSize the size of the pages inside this buffer
     * @param numPages the number of pages
     */
    public PageCacheImpl(PageSize pageSize, int numPages) {
        PAGE_SIZE = pageSize;
        NUM_PAGES = numPages;
        parameterP = 0;
        pAdjustmentEligible = true;
        //buffers = new byte[NUM_PAGES][PAGE_SIZE.getNumberOfBytes()];
        pages = new CacheableData[NUM_PAGES];
        emptyPages = new boolean[NUM_PAGES];
        Arrays.fill(emptyPages, true);

        // Initialize the lists
        topRecentEntriesT1 = new LinkedList<>();
        topFrequentEntriesT2 = new LinkedList<>();
        bottomRecentEntriesB1 = new LinkedList<>();
        bottomFrequentEntriesB2 = new LinkedList<>();
        lists = new ArrayList<>(4);
        lists.add(topRecentEntriesT1);
        lists.add(topFrequentEntriesT2);
        lists.add(bottomRecentEntriesB1);
        lists.add(bottomFrequentEntriesB2);
    }

    /**
     * Checks, if a page is in the cache and returns the cache entry for it. The
     * page is identified by its resource id and the page number. If the
     * requested page is not in the cache, this method returns null.
     * The page experiences a cache hit through the request.
     *
     * @param resourceId The id of the resource for which we seek to get a page.
     * @param pageNumber The physical page number of the page we seek to retrieve.
     * @return The cache entry containing the page data, or null, if the page is not
     * contained in the cache.
     */
    @Override
    public CacheableData getPage(int resourceId, int pageNumber) {
        return getPageAndMaybePin(resourceId, pageNumber, false);
    }

    /**
     * Checks, if a page is in the cache and returns the cache entry for it. Upon success,
     * it also increases the pinning counter of the entry for the page such that the entry
     * cannot be evicted from the cache until the pinning counter reaches zero.
     * <p>
     * The page is identified by its resource id and the page number. If the
     * requested page is not in the cache, this method returns null.
     * The page experiences a cache hit through the request.
     *
     * @param resourceId The id of the resource for which we seek to get a page.
     * @param pageNumber The physical page number of the page we seek to retrieve.
     * @return The cache entry containing the page data, or null, if the page is not
     * contained in the cache.
     */
    @Override
    public CacheableData getPageAndPin(int resourceId, int pageNumber) {
        return getPageAndMaybePin(resourceId, pageNumber, true);
    }


    /**
     * Combined {@link #addPage} or {@link #addPageAndPin}
     */
    public CacheableData getPageAndMaybePin(int resourceId, int pageNumber, boolean pin) {

        EntryId entryIdToGet = new EntryId(resourceId, pageNumber);
        // Search entry in the lists T1 and T2
        if (topRecentEntriesT1.contains(entryIdToGet)){
            for(EntryId entryId : topRecentEntriesT1) {
                // If the entry is in the list, return the page
                if (entryId.equals(entryIdToGet) && !entryId.isExpelled()){
                    //counts as a hit -> move to T2 or top of T1 if it was a prefetched page
                    topRecentEntriesT1.remove(entryId);

                    if (entryId.isRequestedBefore()) topFrequentEntriesT2.addLast(entryId);
                    else {
                        entryId.setRequestedBefore();
                        topRecentEntriesT1.addLast(entryId);
                    }

                    if (pin) entryId.setPinned(true);

                    return pages[entryId.getPositionInCacheBuffer()];
                }
            }
        }

        if (topFrequentEntriesT2.contains(entryIdToGet)){
            for(EntryId entryId : topFrequentEntriesT2) {
                // If the entry is in the list, return the page
                if (entryId.equals(entryIdToGet) && !entryId.isExpelled()){

                    topFrequentEntriesT2.remove(entryId);
                    topFrequentEntriesT2.addLast(entryId); //put to MRU position of T2

                    if (pin) entryId.setPinned(true);

                    return pages[entryId.getPositionInCacheBuffer()];
                }
            }
        }

        return null;
    }

    /**
     * This method adds a page to the cache by adding a cache entry for it. The entry must not be
     * already contained in the cache. In order to add the new entry, one entry will always be
     * evicted to keep the number of contained entries constant.
     * <p>
     * If the cache is still pretty cold (new) then the evicted entry may be an
     * entry containing no resource data. However, the evicted entry always contain a binary page
     * buffer.
     * <p>
     * For ARC, if the page was not in any of the bottom lists (B lists), the page enters the MRU end of the
     * T1 list. The newly added page is not considered as hit yet. Therefore, the first getPage() request to that
     * page produces the first hit and does not move the page out of the T1 list. This functionality is important
     * to allow pre-fetched pages to be added to the cache without causing the first actual "GET" request to
     * mark them as frequent.
     * If the page was contained in any of the Bottom lists before, it will directly enter the T2 list (frequent
     * items). Since it is directly considered frequent, it is considered to be hit.
     *
     * @param newPage    The new page to be put into the cache.
     * @param resourceId The id of the resource the page belongs to.
     * @return The evicted cache entry.
     * @throws CachePinnedException         Thrown, if no page could be evicted, because all pages
     *                                      are pinned.
     * @throws DuplicateCacheEntryException Thrown, if an entry for that page is already contained.
     *                                      The entry is considered to be already contained, if an
     *                                      entry with the same resource-type, resource-id and page
     *                                      number is in the cache. The contents of the binary page
     *                                      buffer does not need to match for this exception to be
     *                                      thrown.
     */
    @Override
    public EvictedCacheEntry addPage(CacheableData newPage, int resourceId) throws CachePinnedException, DuplicateCacheEntryException {
        return addPageAndMaybePin(newPage, resourceId, false);
    }


    /**
     * saves the data of a new page in the buffer of this cache and returns the data of the old page that was stored there.
     * Updates cacheState that keeps track which part of the buffer is used by which page.
     * @param entryId the entryId of the page that will be stored
     * @param newPage the data object
     * @return the data of the byte[] that was stored in the same place before.
     */
    private EvictedCacheEntry loadDataIntoCache(EntryId entryId, CacheableData newPage){
        int pointerCacheBuffer = getEmptyPagePointer();

        //get entry of the Cacheabledata object that gets evicted. There is an entry, iff the cacheableData object != null
        EntryId evictedEntry = null;
        for (LinkedList<EntryId> llist : lists){
            for (EntryId entryId1: llist){
                if (entryId1.getPositionInCacheBuffer() == pointerCacheBuffer){
                    evictedEntry = entryId1;
                }
            }
        }

        EvictedCacheEntry evictedCachePage;
        //Right now this cache position was empty
        if (pages[pointerCacheBuffer] == null) evictedCachePage = new EvictedCacheEntry(new byte[PAGE_SIZE.getNumberOfBytes()]);

        //copy of the byte[] of the old page that was stored at this cache place
        else {
            assert evictedEntry != null;
            evictedCachePage = new EvictedCacheEntry(pages[pointerCacheBuffer].getBuffer(),
                    pages[pointerCacheBuffer], evictedEntry.getResourceId());
            //after we used the entrys positionInCacheBuffer to find it and get the right resourceId, we set the var
            // to -1 as its corresponding page is not in the cache anymore
            evictedEntry.setPositionInCacheBuffer(-1);
        }


        emptyPages[pointerCacheBuffer] = false;

        // Set the buffer pointer in the entry
        entryId.setPositionInCacheBuffer(pointerCacheBuffer);

        //set the data of this place of the caches buffer to the data of the newPage
        pages[pointerCacheBuffer] = newPage;

        return evictedCachePage;
    }

    /**
     * adjusts the parameter p depending on the bottom list that contained the entryId
     * @param entryId the entryId of the page that is now loaded into the cache from B1 or B2
     */
    private void adjustParameterP(EntryId entryId){

        if (!pAdjustmentEligible) return;

        // CASE A: Page P was in B1
        if (bottomRecentEntriesB1.contains(entryId)){
            int delta1;
            if (bottomRecentEntriesB1.size() >= bottomFrequentEntriesB2.size()) delta1 = 1;
            else delta1 = bottomFrequentEntriesB2.size() / bottomRecentEntriesB1.size();
            parameterP = Math.min(parameterP + delta1, NUM_PAGES);
        }// CASE B: Page P was in B2
        else {
            int delta2;
            if (bottomFrequentEntriesB2.size() >= bottomRecentEntriesB1.size()) delta2 = 1;
            else delta2 = bottomRecentEntriesB1.size() / bottomFrequentEntriesB2.size();
            parameterP = Math.max(parameterP - delta2, 0);
        }
    }


    /**
     * Removes a page from B1 or B2 depending on some cases. If it is supposed to remove from B1 but B1 is empty,
     * it removes an entry from t1 and deletes it from the cache
     */
    private void removeTrackedPageFromB1orB2() throws CachePinnedException {
        //1. Remove a tracked page B from B1 or B2
        HashSet<EntryId> hashSet1 = new HashSet<>();
        hashSet1.addAll(topRecentEntriesT1);
        hashSet1.addAll(bottomRecentEntriesB1);

        HashSet<EntryId> hashSet2 = new HashSet<>();
        hashSet2.addAll(topRecentEntriesT1);
        hashSet2.addAll(topFrequentEntriesT2);
        hashSet2.addAll(bottomRecentEntriesB1);
        hashSet2.addAll(bottomFrequentEntriesB2);

        //CASE I: |T1| + |B1| = c and |T1| < c -> Remove LRU from B1
        if (hashSet1.size() == NUM_PAGES && topRecentEntriesT1.size() < NUM_PAGES){
        //if (topRecentEntriesT1.size() + bottomRecentEntriesB1.size() == NUM_PAGES && topRecentEntriesT1.size() < NUM_PAGES){
            bottomRecentEntriesB1.removeFirst(); //entries in a bottom list cant be pinned
        }// CASE II: |T1| = c and |B1| is empty -> Evict LRU from T1 from the cache
        else if (topRecentEntriesT1.size() == NUM_PAGES && bottomRecentEntriesB1.isEmpty()){
            EntryId removedEntry = removeFirstEligible(topRecentEntriesT1);
            if (removedEntry != null){
                emptyPages[removedEntry.getPositionInCacheBuffer()] = true;
                bottomRecentEntriesB1.addLast(removedEntry); //needed for test, but idk how it makes sense, because now we
                // didnt make space in any bottom list
            }
            else throw new CachePinnedException();

        }
        // CASE III: |T1 U B1| < c and |T1 U T2 U B1 U B2| = 2c -> Remove LRU from B2
        else if (hashSet1.size() < NUM_PAGES && hashSet2.size() == 2 * NUM_PAGES){
        //else if (topRecentEntriesT1.size() + bottomRecentEntriesB1.size() < NUM_PAGES &&
        //        topRecentEntriesT1.size() + topFrequentEntriesT2.size() + bottomRecentEntriesB1.size() + bottomFrequentEntriesB2.size() == 2 * NUM_PAGES){
            bottomFrequentEntriesB2.removeFirst();
        }
    }

    /**
     * If there is enough empty space currently in the cache, this method does nothing.
     * Otherwise, it removes a page from the cache according to the rules of T1, T2 and the parameter p.
     * It removes the entry of the evicted page from t1 or t2 and puts it into b1 or b2 accordingly
     * As a side effect it sets the parameter pAdjustmentEligible to false, if it had to remove a page from the wrong list
     * @param entryId The entry that resulted in a cache miss. It is used to decide how to adjust the parameter p.
     * @throws CachePinnedException if the cache is full and there is no unpinned page to evict.
     */
    private void evictPageFromCacheAndT1orT2(EntryId entryId) throws CachePinnedException {
        // Evict a page E from T1 or T2 to make space for a new page P (case 2 or case 3)

        pAdjustmentEligible = true;

        // Case X: There is already enough space
        if (topRecentEntriesT1.size() + topFrequentEntriesT2.size() < NUM_PAGES){
            return;
        }

        // CASE a: T1 is not empty AND (|T1| > p OR (Page P is in B2 AND |T1| = p)) -> Remove LRU of T1 from the cache and more tracked page to MRU of B1
        if (!topRecentEntriesT1.isEmpty() && (topRecentEntriesT1.size() > parameterP ||
                (bottomFrequentEntriesB2.contains(entryId) && topRecentEntriesT1.size() == parameterP))){

            EntryId removedEntry = removeFirstEligible(topRecentEntriesT1);
            if (removedEntry == null) { // everything in T1 is pinned
                pAdjustmentEligible = false;
                removedEntry = removeFirstEligible(topFrequentEntriesT2);
            }
            if (removedEntry == null) throw new CachePinnedException(); // everything in T2 is pinned as well

            emptyPages[removedEntry.getPositionInCacheBuffer()] = true; // this position in cache is now considered empty

            if (pAdjustmentEligible) bottomRecentEntriesB1.addLast(removedEntry); // we removed an entry from t1
            else bottomFrequentEntriesB2.addLast(removedEntry); // we removed an entry from t2

        }
        // CASE b: (otherwise)
        else {
            EntryId removedEntry = removeFirstEligible(topFrequentEntriesT2);
            if (removedEntry == null) { // everything in T2 is pinned
                pAdjustmentEligible = false;
                removedEntry = removeFirstEligible(topRecentEntriesT1);
            }
            if (removedEntry == null) throw new CachePinnedException(); // everything in T1 is pinned as well

            emptyPages[removedEntry.getPositionInCacheBuffer()] = true; // this position in cache is now considered empty

            if (pAdjustmentEligible) bottomFrequentEntriesB2.addLast(removedEntry); // we removed an entry from t2
            else bottomRecentEntriesB1.addLast(removedEntry); // we removed an entry from t1
        }
    }

    /**
     * removes the LRU item that is not pinned from the list l
     * @param l list containing entryId that are sorted from LRU to MRU and some entries might be pinned
     * @return the LRU item that is not pinned
     */
    private EntryId removeFirstEligible(List<EntryId> l){
        for (int i = 0; i < l.size(); i++) {
            if (!l.get(i).isPinned()){
                return l.remove(i);
            }
        }
        return null;
    }


    /**
     * Method to get the first free buffer pointer in the PageCache buffer.
     * If the buffer is full, return -1.
    **/
    public int getEmptyPagePointer(){
        for(int i = 0; i < NUM_PAGES; i++){
            if(emptyPages[i]){
                return i;
            }
        }
        return -1;
    }

    /**
     * This method behaves similarly to the {@link #addPage} method, with the
     * following distinctions:
     *
     * <ol>
     * <li>The page is immediately pinned. (Increase pinning counter, see unpinPage for further information.)</li>
     * <li>The page is immediately considered to be hit, even if it enters the T1 list.</li>
     * </ol>
     *
     * @param newPage    The new page to be put into the cache.
     * @param resourceId The id of the resource the page belongs to.
     * @return The evicted cache entry.
     * @throws CachePinnedException         Thrown, if no page could be evicted, because all pages
     *                                      are pinned.
     * @throws DuplicateCacheEntryException Thrown, if an entry for that page is already contained.
     *                                      The entry is considered to be already contained, if an
     *                                      entry with the same resource-type, resource-id and page
     *                                      number is in the cache. The contents of the binary page
     *                                      buffer does not need to match for this exception to be
     *                                      thrown.
     */
    @Override
    public EvictedCacheEntry addPageAndPin(CacheableData newPage, int resourceId) throws CachePinnedException, DuplicateCacheEntryException {
        return addPageAndMaybePin(newPage, resourceId, true);
    }

    /**
     * Combined {@link #addPage} or {@link #addPageAndPin}
     */
    public EvictedCacheEntry addPageAndMaybePin(CacheableData newPage, int resourceId, boolean pin) throws CachePinnedException, DuplicateCacheEntryException {

        EntryId entryId = new EntryId(resourceId, newPage.getPageNumber(), pin);
        if (pin) entryId.setRequestedBefore();

        // Check if the page is already in the cache
        if(topRecentEntriesT1.contains(entryId) || topFrequentEntriesT2.contains(entryId)){
            throw new DuplicateCacheEntryException(resourceId, newPage.getPageNumber());
        }

        // CASE 2: Page P is either in B1 or B2 -> 1. Evict a page E from T1 or T2; 2. Adapt parameter p;
        // 3. Load data of page P into the cache; 4. Move (id of) page P to MRU of T2 (page is now frequent)
        else if (bottomRecentEntriesB1.contains(entryId) || bottomFrequentEntriesB2.contains(entryId)) {
            // 1. Evict a page E from T1 or T2 to make space for a new page P (case 2 or case 3)
            evictPageFromCacheAndT1orT2(entryId);

            // 2. Adjust parameter p when a page P was not in the cache but tracked (case 2)
            adjustParameterP(entryId);

            // 3. Load data of page p into the cache
            EvictedCacheEntry evictedCacheEntry = loadDataIntoCache(entryId, newPage);

            // 4. Move (id of) page P to MRU of T2 (page is now frequent)
            if (bottomRecentEntriesB1.contains(entryId) && !entryId.isRequestedBefore()) {
                entryId.setRequestedBefore();
                topRecentEntriesT1.addLast(entryId);
            }
            else topFrequentEntriesT2.addLast(entryId);

            return evictedCacheEntry;

        }

        // CASE 3: Page P not in any list -> 1. Remove a tracked page B from B1 or B2; 2. Evict a page E from T1 or T2;
        // 3. Load data of page P into the cache; 4. Move page P to MRU of T1 (page is now recent)
        else {
            //1. Remove a tracked page B from B1 or B2
            removeTrackedPageFromB1orB2();

            //2. Evict a page E from T1 or T2;
            evictPageFromCacheAndT1orT2(entryId);

            //3. Load data of page P into the cache
            EvictedCacheEntry evictedCacheEntry = loadDataIntoCache(entryId, newPage);

            //4. Move page P to MRU of T1 (page is now recent)
            topRecentEntriesT1.addLast(entryId);

            return evictedCacheEntry;
        }
    }

    /**
     * Decreases the pinning counter of the entry for the page described by this resource ID and
     * page number. If there is no entry for this page, this method does nothing. If
     * the entry is not pinned, this method does nothing. If the pinning counter reaches zero, the
     * page can be evicted from the cache.
     *
     * @param resourceId The id of the resource of the page entry to be unpinned.
     * @param pageNumber The physical page number of the page entry to be unpinned.
     */
    @Override
    public void unpinPage(int resourceId, int pageNumber) {
        EntryId entryIdToUnpin = new EntryId(resourceId, pageNumber);
        // Search the entry in the lists and mark it as unpinned
        for (LinkedList<EntryId> list : lists) {
            for(EntryId entryId : list) {
                if (entryId.equals(entryIdToUnpin)){
                    entryId.setPinned(false);
                }
            }
        }
    }

    /**
     * Gets all pages/entries for the resource specified by this resource type and
     * resource ID. A call to this method counts as a request for each individual pages
     * entry in the sense of how it affects the replacement strategy.
     *
     * @param resourceId The id of the resource for which we seek the pages.
     * @return An array of all pages for this resource, or an empty array, if no page is
     * contained for this resource.
     */
    @Override
    public CacheableData[] getAllPagesForResource(int resourceId) {
        ArrayList<CacheableData> tempRes = new ArrayList<>();
        ArrayList<EntryId> tempResT1 = new ArrayList<>();
        ArrayList<EntryId> tempResT2 = new ArrayList<>();


        for (EntryId entryId : topRecentEntriesT1) {
            if (entryId.getResourceId() == resourceId){
                tempRes.add(pages[entryId.getPositionInCacheBuffer()]);
                tempResT1.add(entryId);
            }
        }

        for (EntryId entryId : topFrequentEntriesT2) {
            if (entryId.getResourceId() == resourceId){
                tempRes.add(pages[entryId.getPositionInCacheBuffer()]);
                tempResT2.add(entryId);
            }
        }


        topRecentEntriesT1.removeAll(tempResT1);
        topFrequentEntriesT2.removeAll(tempResT2);

        //change lists accordingly
        for (EntryId entryIdt1 : tempResT1){
            if (entryIdt1.isRequestedBefore()){
                topFrequentEntriesT2.addLast(entryIdt1);
            }
            else {
                entryIdt1.setRequestedBefore();
                topRecentEntriesT1.addLast(entryIdt1);
            }
        }

        for (EntryId entryIdt2 : tempResT2){
            topFrequentEntriesT2.addLast(entryIdt2);
        }



        CacheableData[] res = new CacheableData[tempRes.size()];
        return tempRes.toArray(res);
    }

    /**
     * Removes all entries/pages belonging to a specific resource from the cache. This method
     * does not obey pinning: Entries that are pinned are also expelled. The expelled pages
     * may no longer be found by the {@link #getPage} or {@link #getPageAndPin}.
     * The entries for the expelled pages will be the next to be evicted from the cache.
     * If no pages from the given resource are contained in the cache, this method does
     * nothing.
     * <p>
     * NOTE: This method must not change the size of the cache. It also is not required to
     * physically destroy the entries for the expelled pages - they simply must no longer
     * be retrievable from the cache and be the next to be replaced. The byte arrays
     * behind the expelled pages must be kept in the cache and be returned as evicted
     * entries once further pages are added to the cache.
     *
     * @param resourceId The id of the resource whose pages are to be replaced.
     */
    @Override
    public void expelAllPagesForResource(int resourceId) {


        for(LinkedList<EntryId> list : lists){

            ArrayList<EntryId> toRemove = new ArrayList<>();

            for (EntryId entryId : list){
                if (entryId.getResourceId() == resourceId){
                    entryId.setExpelled(true);
                    entryId.forceUnpin();
                    toRemove.add(entryId);
                    emptyPages[entryId.getPositionInCacheBuffer()] = true;//this cache position is empty
                }
            }
            list.removeAll(toRemove);
            list.addAll(0, toRemove);
        }


    }

    /**
     * Gets the capacity of this cache in pages (entries).
     * @return The number of pages (cache entries) that this cache can hold.
     */
    @Override
    public int getCapacity() {
        return NUM_PAGES;
    }

    /**
     * Unpins all entries, such that they can now be evicted from the cache (pinning counter = 0).
     * This operation has no impact on the position of the entry in the structure that
     * decides which entry to evict next.
     */
    @Override
    public void unpinAllPages() {
        for (LinkedList<EntryId> list : lists) {
            for(EntryId entryId : list) {
                entryId.forceUnpin();
            }
        }
    }


    @Override
    public String toString() {
        return "PageCacheImpl{" +
                "PAGE_SIZE=" + PAGE_SIZE +
                ", NUM_PAGES=" + NUM_PAGES +
                ", parameterP=" + parameterP +
                //", \npages=" + Arrays.toString(pages) +
                ", \nemptyPages(" + emptyPages.length + ")=" + Arrays.toString(emptyPages) +
                ", \ntopRecentEntriesT1(" + topRecentEntriesT1.size() + ")=" + topRecentEntriesT1 +
                ", \ntopFrequentEntriesT2(" + topFrequentEntriesT2.size() + ")=" + topFrequentEntriesT2 +
                ", \nbottomRecentEntriesB1(" + bottomRecentEntriesB1.size() + ")=" + bottomRecentEntriesB1 +
                ", \nbottomFrequentEntriesB2(" + bottomFrequentEntriesB2.size() + ")=" + bottomFrequentEntriesB2 +
                '}';
    }
}
