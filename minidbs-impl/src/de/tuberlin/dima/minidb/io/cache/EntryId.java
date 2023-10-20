package de.tuberlin.dima.minidb.io.cache;

/**
 * Helper class to track resource ids and page numbers of cache entries.
 * Copied from the professor helper class in the test, might be helpful to
 * keep track of resource id, page number, buffer pointer and pinning of
 * the different cache entries.
 */
public class EntryId
{
    /**
     * the resource id of the page
     */
    private final int RESOURCE_ID;

    /**
     * the page number of the page
     */
    private final int PAGE_NUMBER;

    /**
     * Pinning is managed by a pinning counter. If a page is pinned the counter and if the counter is 0 it is not pinned anymore.
     */
    private int pinningCounter;

    /**
     * where this page is stored in the cache (-1 for not stored)
     */
    private int positionInCacheBuffer;

    /**
     * if this page was requested before. Needed to apply prefetching logic
     */
    private boolean requestedBefore;

    /**
     * set if this page is supposed to be removed. Cant be accessed anymore, but when it gets deleted we still
     * need it's buffer to create an EvictedCacheEntry
     */
    private boolean expelled;


    public EntryId(int id, int pageNumber)
    {
        this(id, pageNumber, false, false);
    }

    public EntryId(int id, int pageNumber, boolean pinned)
    {
        this(id, pageNumber, pinned, false);
    }

    /**
     * constructor for entryId. get used by other constructors of this class as well
     * @param id the resource id
     * @param pageNumber the page number
     * @param pinned if the page is pinned
     * @param requestedBefore if the page was requested before (true for a page that gets added and pinned,
     *                        as this method counts as a request)
     */
    public EntryId(int id, int pageNumber, boolean pinned, boolean requestedBefore)
    {
        this.RESOURCE_ID = id;
        this.PAGE_NUMBER = pageNumber;
        if (pinned) pinningCounter = 1;
        else pinningCounter = 0;
        this.requestedBefore = requestedBefore;
        expelled = false;
        positionInCacheBuffer = -1;
    }

    /**
     * Returns the page number of this entry id.
     *
     * @return The page number.
     */
    public int getPageNumber()
    {
        return this.PAGE_NUMBER;
    }


    /**
     * Returns the resource id of this entry id.
     *
     * @return The resource id.
     */
    public int getResourceId()
    {
        return this.RESOURCE_ID;
    }

    /**
     * Increases the pinning counter by 1 if pinned is true.
     * Decreases the pinning counter by 1 if pinned is false. (Minimum 0)
     */
    public void setPinned(boolean pinned) {
        if (pinned) pinningCounter++;
        else {
            if (pinningCounter > 0) pinningCounter--;
        }
    }

    /**
     * Sets the pinning counter to 0
     */
    public void forceUnpin(){
        pinningCounter = 0;
    }

    /**
     * Returns the state of the entry (pinned or not).
     */
    public boolean isPinned()
    {
        return pinningCounter > 0;
    }

    /**
     * Returns the buffer page number of this entry id.
     */
    public int getPositionInCacheBuffer() {
        return positionInCacheBuffer;
    }

    /**
     * Sets the pointer of this entry id, that indicates where this entry is located in the cache.
     * @param positionInCacheBuffer The buffer page number.
     */
    public void setPositionInCacheBuffer(int positionInCacheBuffer)
    {
        this.positionInCacheBuffer = positionInCacheBuffer;
    }

    /**
     * Returns the expelled state of the entry (expelled or not)
     */
    public boolean isExpelled() {
        return expelled;
    }

    /**
     * Sets the expelled state to true or false
     */
    public void setExpelled(boolean expelled) {
        this.expelled = expelled;
    }

    /**
     * indicates if this entry was requested before, if yes it will go to T2
     */
    public boolean isRequestedBefore() {
        return requestedBefore;
    }

    /**
     * Sets requested before to true
     */
    public void setRequestedBefore() {
        this.requestedBefore = true;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.RESOURCE_ID;
        result = prime * result + this.PAGE_NUMBER;
        return result;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EntryId entryId = (EntryId) o;
        return RESOURCE_ID == entryId.RESOURCE_ID && PAGE_NUMBER == entryId.PAGE_NUMBER;
    }

    @Override
    public String toString() {
        return "EntryId{" +
                "RESOURCE_ID=" + RESOURCE_ID +
                ", PAGE_NUMBER=" + PAGE_NUMBER +
                ", pins=" + pinningCounter +
                ", pos=" + positionInCacheBuffer +
                ", reqBefore=" + requestedBefore +
                ", expelled=" + expelled +
                '}';
    }
}