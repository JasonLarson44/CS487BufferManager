package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.util.HashMap;

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager manages an array of main memory pages.  The array is
 * called the buffer pool, each page is called a frame.  
 * It provides the following services:
 * <ol>
 * <li>Pinning and unpinning disk pages to/from frames
 * <li>Allocating and deallocating runs of disk pages and coordinating this with
 * the buffer pool
 * <li>Flushing pages from the buffer pool
 * <li>Getting relevant data
 * </ol>
 * The buffer manager is used by access methods, heap files, and
 * relational operators.
 */
public class BufMgr implements GlobalConst {

  protected Page[] BufferPool; //Pages currently loaded into memory, correspond to frame table for position in array
  //If page number maps to a frame desc that has a frame number. Frame number should be
  //the position of the page in the buffer pool array
  protected FrameDesc[] FrameTab; //Store frame descriptions, number frams 1 - poolsize
  protected HashMap<PageId, FrameDesc> Map; //Map pageno to their frame desc
  protected Clock replPolicy;
  protected int PoolSize; //BufferPool size
  protected int currentFrame;

  /**
   * Constructs a buffer manager by initializing member data.  
   * 
   * @param numframes number of frames in the buffer pool
   */
  public BufMgr(int numframes) {

    this.PoolSize = numframes;
    this.BufferPool = new Page[numframes];
    this.FrameTab = new FrameDesc[numframes];
    for(int i = 0; i < numframes; ++i)//Number the frame table 0 -> numframes -1
    {
      this.FrameTab[i] = new FrameDesc();
      this.BufferPool[i] = new Page();
      this.FrameTab[i].setFrameNum(i);
    }
    this.Map = new HashMap<PageId, FrameDesc>(numframes); //Hashmap needs to be big enough to map one page to one frame
    this.currentFrame = 0;//set current frame to first frame
    this.replPolicy = new Clock();

  } // public BufMgr(int numframes)

  /**
   * The result of this call is that disk page number pageno should reside in
   * a frame in the buffer pool and have an additional pin assigned to it, 
   * and mempage should refer to the contents of that frame. <br><br>
   * 
   * If disk page pageno is already in the buffer pool, this simply increments 
   * the pin count.  Otherwise, this<br> 
   * <pre>
   * 	uses the replacement policy to select a frame to replace
   * 	writes the frame's contents to disk if valid and dirty
   * 	if (contents == PIN_DISKIO)
   * 		read disk page pageno into chosen frame
   * 	else (contents == PIN_MEMCPY)
   * 		copy mempage into chosen frame
   * 	[omitted from the above is maintenance of the frame table and hash map]
   * </pre>		
   * @param pageno identifies the page to pin
   * @param mempage An output parameter referring to the chosen frame.  If
   * contents==PIN_MEMCPY it is also an input parameter which is copied into
   * the chosen frame, see the contents parameter. 
   * @param contents Describes how the contents of the frame are determined.<br>  
   * If PIN_DISKIO, read the page from disk into the frame.<br>  
   * If PIN_MEMCPY, copy mempage into the frame.<br>  
   * If PIN_NOOP, copy nothing into the frame - the frame contents are irrelevant.<br>
   * Note: In the cases of PIN_MEMCPY and PIN_NOOP, disk I/O is avoided.
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned.
   * @throws IllegalStateException if all pages are pinned (i.e. pool is full)
   */
  public void pinPage(PageId pageno, Page mempage, int contents) {

    FrameDesc frame = this.Map.get(pageno); //get the frame desc if the page is in the buffer pool
    int FrameNum = 0;//Find the framedesc to alter
    if(frame != null) //If the page is in the buffer pool, alter the FrameDesc related to it.
    {
      this.FrameTab[frame.FrameNum].incrementPinCount(); //Page is already in a frame, increment pin count
    }
    else //Else attempt to add a new framedesc if the map is not full yet
    {
      if (Map.size() < this.PoolSize) //room to add
      {
        FrameNum = currentFrame;
        currentFrame += 1;
      }
      else //No room to add, run replacement to select which page to evict
      {
        if(getNumUnpinned() == 0)
        {
          throw new IllegalStateException("all pages are pinned");
        }
        FrameNum = replPolicy.selectVictim(this.FrameTab, this.PoolSize);
        if(FrameNum == -1){//could not select a page to replace
          throw new IllegalStateException("All pages are pinned");
        }

        else{//write old page to disk and fill frame
          frame = FrameTab[FrameNum];
          flushPage(frame.pageNum);//write to disk if dirty
          Map.remove(frame.pageNum);
        }

      }
      //Time to read in page....
      if(contents == PIN_DISKIO){//If we need to read the page in from disk
        Minibase.DiskManager.read_page(pageno, BufferPool[FrameNum]);
        FrameTab[FrameNum].setValidBit(true);
      }
      else if(contents == PIN_MEMCPY){ //Copy mempage into buffer pool frame
        BufferPool[FrameNum].copyPage(mempage);
        FrameTab[FrameNum].setValidBit(true);
      }
      else{ //Data is invalid
        FrameTab[FrameNum].setValidBit(false);
      }

      //add to hashmap and update frame desc
      FrameTab[FrameNum].setPageNo(pageno);
      Map.put(FrameTab[FrameNum].pageNum, FrameTab[FrameNum]); //Add key value mapping from page no to its frame description to the hash map
      FrameTab[FrameNum].setDirtyBit(false);
      FrameTab[FrameNum].setPinCount(1);

    }

  } // public void pinPage(PageId pageno, Page page, int contents)
  
  /**
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
   * @throws IllegalArgumentException if the page is not in the buffer pool
   *  or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) {

    FrameDesc toUnpin = Map.get(pageno);
    if(toUnpin != null)
    {
      toUnpin.decrementPinCount();
      if(dirty)
      {
        toUnpin.setDirtyBit(dirty);
      }
    }
    else {
      throw new IllegalArgumentException("Page is not pinned");
    }
  } // public void unpinPage(PageId pageno, boolean dirty)
  
  /**
   * Allocates a run of new disk pages and pins the first one in the buffer pool.
   * The pin will be made using PIN_MEMCPY.  Watch out for disk page leaks.
   * 
   * @param firstpg input and output: holds the contents of the first allocated page
   * and refers to the frame where it resides
   * @param run_size input: number of pages to allocate
   * @return page id of the first allocated page
   * @throws IllegalArgumentException if firstpg is already pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
  public PageId newPage(Page firstpg, int run_size) {

    PageId pgid = Minibase.DiskManager.allocate_page(run_size);
    FrameDesc frame = Map.get(pgid);
    if(getNumUnpinned() == 0){
      throw new IllegalStateException("Pool exceeded");
    }
    if(frame != null && frame.getPinCount() != 0) {
      throw new IllegalArgumentException("That page is already pinned");
    }
    pinPage(pgid, firstpg, PIN_MEMCPY);
    return pgid;
  } // public PageId newPage(Page firstpg, int run_size)

  /**
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) {
    FrameDesc frame = Map.get(pageno);
    if(frame != null && frame.getPinCount() != 0) {
      throw new IllegalArgumentException("The page is pinned");
    }


  } // public void freePage(PageId firstid)

  /**
   * Write all valid and dirty frames to disk.
   * Note flushing involves only writing, not unpinning or freeing
   * or the like.
   * 
   */
  public void flushAllFrames() {

    for(int i  = 0; i < PoolSize; ++i){
      if(FrameTab[i].pageNum.pid != INVALID_PAGEID) {
        flushPage(FrameTab[i].pageNum);
      }
    }

  } // public void flushAllFrames()

  /**
   * Write a page in the buffer pool to disk, if dirty.
   * 
   * @throws IllegalArgumentException if the page is not in the buffer pool
   */
  public void flushPage(PageId pageno) {

	FrameDesc frame = Map.get(pageno);//Find the frame desc to check if page is dirty
    if(frame == null) {//page is not in buffer pool
      throw new IllegalArgumentException("The page is not in the buffer pool");
    }
    if(frame.isDirty() && frame.isValid())//write it to disk
    {
      Minibase.DiskManager.write_page(pageno, BufferPool[frame.getFrameNum()]);//the page in buffer pool should
      //be in the same position of the buffer pool array as the frame table
    }
    
  }

   /**
   * Gets the total number of buffer frames.
   */
  public int getNumFrames() {
    return this.PoolSize;
  }

  /**
   * Gets the total number of unpinned buffer frames.
   */
  public int getNumUnpinned() {
    int count = 0;
    for(int i = 0; i < FrameTab.length; ++i)
    {
      if(FrameTab[i].PinCount < 1)
        count += 1;
    }
    return count;
  }

} // public class BufMgr implements GlobalConst
